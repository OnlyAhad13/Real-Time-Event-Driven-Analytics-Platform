"""
Real-Time Analytics Platform - Data Quality & Observability
============================================================
Production-grade data quality checks for the Silver Lake.

Checks performed:
1. Freshness: Are the latest files from today?
2. Volume: Is the row count within 20% of the 7-day average?
3. Nulls: Are critical columns (user_id, timestamp) 100% populated?

Usage:
    python data_quality_checks.py [--date YYYY-MM-DD] [--slack-webhook URL]
"""

import os
import sys
import json
import logging
import argparse
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

import pandas as pd
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

# ============================================
# Logging Configuration
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataQuality")

# ============================================
# Configuration
# ============================================

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password_change_me")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Silver Lake Configuration
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "events/")

# Quality thresholds
VOLUME_THRESHOLD_PCT = float(os.getenv("VOLUME_THRESHOLD_PCT", "20"))  # 20% deviation
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))  # 7-day average

# Critical columns that must be 100% populated
CRITICAL_COLUMNS = ["user_id", "session_start"]  # Adjusted for Silver schema

# ============================================
# Data Classes
# ============================================

class CheckStatus(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""
    check_name: str
    status: CheckStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class QualityReport:
    """Aggregated quality report."""
    target_date: str
    checks: List[QualityCheckResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        return all(c.status == CheckStatus.PASSED for c in self.checks)
    
    @property
    def failed_checks(self) -> List[QualityCheckResult]:
        return [c for c in self.checks if c.status == CheckStatus.FAILED]
    
    def to_dict(self) -> dict:
        return {
            "target_date": self.target_date,
            "overall_status": "PASSED" if self.passed else "FAILED",
            "total_checks": len(self.checks),
            "passed_checks": len([c for c in self.checks if c.status == CheckStatus.PASSED]),
            "failed_checks": len(self.failed_checks),
            "checks": [c.to_dict() for c in self.checks],
        }

# ============================================
# Alert System
# ============================================

class AlertSystem:
    """Mock alert system with console and Slack support."""
    
    def __init__(self, slack_webhook: Optional[str] = None):
        self.slack_webhook = slack_webhook
    
    def send_alert(self, report: QualityReport):
        """Send alert for failed checks."""
        if report.passed:
            logger.info("✅ All quality checks passed. No alerts needed.")
            return
        
        # Console alert
        self._console_alert(report)
        
        # Slack alert (if configured)
        if self.slack_webhook:
            self._slack_alert(report)
    
    def _console_alert(self, report: QualityReport):
        """Print alert to console."""
        logger.warning("=" * 60)
        logger.warning("🚨 DATA QUALITY ALERT 🚨")
        logger.warning("=" * 60)
        logger.warning(f"Date: {report.target_date}")
        logger.warning(f"Failed Checks: {len(report.failed_checks)}")
        
        for check in report.failed_checks:
            logger.warning("-" * 40)
            logger.warning(f"❌ {check.check_name}")
            logger.warning(f"   Message: {check.message}")
            for key, value in check.details.items():
                logger.warning(f"   {key}: {value}")
        
        logger.warning("=" * 60)
    
    def _slack_alert(self, report: QualityReport):
        """Send alert to Slack webhook."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Data Quality Alert",
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Date:*\n{report.target_date}"},
                    {"type": "mrkdwn", "text": f"*Failed Checks:*\n{len(report.failed_checks)}"},
                ]
            },
            {"type": "divider"},
        ]
        
        for check in report.failed_checks:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*❌ {check.check_name}*\n{check.message}"
                }
            })
        
        payload = {"blocks": blocks}
        
        try:
            response = requests.post(
                self.slack_webhook,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if response.status_code == 200:
                logger.info("Slack alert sent successfully")
            else:
                logger.error(f"Failed to send Slack alert: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")

# ============================================
# Data Quality Checker
# ============================================

class DataQualityChecker:
    """Main data quality checker class."""
    
    def __init__(self, target_date: str):
        self.target_date = target_date
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        self.report = QualityReport(target_date=target_date)
    
    def run_all_checks(self) -> QualityReport:
        """Run all quality checks."""
        logger.info(f"Running quality checks for date: {self.target_date}")
        
        # Check 1: Freshness
        self.check_freshness()
        
        # Check 2: Volume
        self.check_volume()
        
        # Check 3: Nulls
        self.check_nulls()
        
        return self.report
    
    def _list_files_for_date(self, date: str) -> List[str]:
        """List all Parquet files for a specific date."""
        prefix = f"{SILVER_PREFIX}date={date}/"
        files = []
        
        try:
            objects = self.client.list_objects(SILVER_BUCKET, prefix=prefix, recursive=True)
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    files.append(obj.object_name)
        except S3Error as e:
            logger.error(f"Error listing files: {e}")
        
        return files
    
    def _get_file_stats(self, date: str) -> Dict[str, Any]:
        """Get statistics for files of a specific date."""
        files = self._list_files_for_date(date)
        stats = {
            "date": date,
            "file_count": len(files),
            "files": files,
        }
        return stats
    
    def _read_parquet_files(self, files: List[str]) -> Optional[pd.DataFrame]:
        """Read Parquet files into a DataFrame."""
        if not files:
            return None
        
        dfs = []
        for file_path in files:
            try:
                # Download to temp file and read
                # For simplicity, we'll use the file path directly if accessible
                # In production, you'd download to temp and read
                response = self.client.get_object(SILVER_BUCKET, file_path)
                import io
                data = io.BytesIO(response.read())
                df = pd.read_parquet(data)
                dfs.append(df)
                response.close()
            except Exception as e:
                logger.warning(f"Error reading {file_path}: {e}")
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return None
    
    # ========================================
    # Check 1: Freshness
    # ========================================
    
    def check_freshness(self):
        """Check if data for target date exists and is recent."""
        check_name = "Freshness Check"
        logger.info(f"Running {check_name}...")
        
        files = self._list_files_for_date(self.target_date)
        
        if files:
            # Check if files were modified today
            latest_modified = None
            for file_path in files:
                try:
                    stat = self.client.stat_object(SILVER_BUCKET, file_path)
                    if latest_modified is None or stat.last_modified > latest_modified:
                        latest_modified = stat.last_modified
                except Exception as e:
                    logger.warning(f"Error checking file stat: {e}")
            
            if latest_modified:
                hours_old = (datetime.now(latest_modified.tzinfo) - latest_modified).total_seconds() / 3600
                
                if hours_old <= 24:
                    result = QualityCheckResult(
                        check_name=check_name,
                        status=CheckStatus.PASSED,
                        message=f"Data is fresh. Latest file modified {hours_old:.1f} hours ago.",
                        details={
                            "file_count": len(files),
                            "latest_modified": latest_modified.isoformat(),
                            "hours_old": round(hours_old, 2),
                        }
                    )
                else:
                    result = QualityCheckResult(
                        check_name=check_name,
                        status=CheckStatus.FAILED,
                        message=f"Data is stale. Latest file is {hours_old:.1f} hours old.",
                        details={
                            "file_count": len(files),
                            "latest_modified": latest_modified.isoformat(),
                            "hours_old": round(hours_old, 2),
                        }
                    )
            else:
                result = QualityCheckResult(
                    check_name=check_name,
                    status=CheckStatus.WARNING,
                    message="Could not determine file modification times.",
                    details={"file_count": len(files)}
                )
        else:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.FAILED,
                message=f"No data files found for date {self.target_date}",
                details={"date": self.target_date, "prefix": f"{SILVER_PREFIX}date={self.target_date}/"}
            )
        
        self.report.checks.append(result)
        logger.info(f"{check_name}: {result.status.value}")
    
    # ========================================
    # Check 2: Volume
    # ========================================
    
    def check_volume(self):
        """Check if row count is within threshold of 7-day average."""
        check_name = "Volume Check"
        logger.info(f"Running {check_name}...")
        
        # Get current date row count
        current_files = self._list_files_for_date(self.target_date)
        current_df = self._read_parquet_files(current_files)
        
        if current_df is None or current_df.empty:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.SKIPPED,
                message=f"No data found for {self.target_date}. Skipping volume check.",
                details={"date": self.target_date}
            )
            self.report.checks.append(result)
            return
        
        current_count = len(current_df)
        
        # Get historical row counts for last N days
        historical_counts = []
        target_dt = datetime.strptime(self.target_date, "%Y-%m-%d")
        
        for i in range(1, LOOKBACK_DAYS + 1):
            hist_date = (target_dt - timedelta(days=i)).strftime("%Y-%m-%d")
            hist_files = self._list_files_for_date(hist_date)
            hist_df = self._read_parquet_files(hist_files)
            if hist_df is not None and not hist_df.empty:
                historical_counts.append(len(hist_df))
        
        if not historical_counts:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.WARNING,
                message="No historical data available for comparison.",
                details={"current_count": current_count}
            )
        else:
            avg_count = sum(historical_counts) / len(historical_counts)
            deviation_pct = abs(current_count - avg_count) / avg_count * 100 if avg_count > 0 else 0
            
            if deviation_pct <= VOLUME_THRESHOLD_PCT:
                result = QualityCheckResult(
                    check_name=check_name,
                    status=CheckStatus.PASSED,
                    message=f"Row count is within threshold. Deviation: {deviation_pct:.1f}%",
                    details={
                        "current_count": current_count,
                        "avg_count": round(avg_count, 2),
                        "deviation_pct": round(deviation_pct, 2),
                        "threshold_pct": VOLUME_THRESHOLD_PCT,
                        "historical_days": len(historical_counts),
                    }
                )
            else:
                result = QualityCheckResult(
                    check_name=check_name,
                    status=CheckStatus.FAILED,
                    message=f"Row count deviation ({deviation_pct:.1f}%) exceeds threshold ({VOLUME_THRESHOLD_PCT}%)",
                    details={
                        "current_count": current_count,
                        "avg_count": round(avg_count, 2),
                        "deviation_pct": round(deviation_pct, 2),
                        "threshold_pct": VOLUME_THRESHOLD_PCT,
                        "historical_counts": historical_counts,
                    }
                )
        
        self.report.checks.append(result)
        logger.info(f"{check_name}: {result.status.value}")
    
    # ========================================
    # Check 3: Nulls
    # ========================================
    
    def check_nulls(self):
        """Check that critical columns have 100% non-null values."""
        check_name = "Null Check"
        logger.info(f"Running {check_name}...")
        
        files = self._list_files_for_date(self.target_date)
        df = self._read_parquet_files(files)
        
        if df is None or df.empty:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.SKIPPED,
                message=f"No data found for {self.target_date}. Skipping null check.",
                details={"date": self.target_date}
            )
            self.report.checks.append(result)
            return
        
        # Check each critical column
        null_stats = {}
        failed_columns = []
        
        for col in CRITICAL_COLUMNS:
            if col in df.columns:
                null_count = df[col].isna().sum()
                total_count = len(df)
                null_pct = (null_count / total_count) * 100 if total_count > 0 else 0
                
                null_stats[col] = {
                    "null_count": int(null_count),
                    "total_count": total_count,
                    "null_pct": round(null_pct, 4),
                }
                
                if null_count > 0:
                    failed_columns.append(col)
            else:
                null_stats[col] = {"error": "Column not found"}
                failed_columns.append(col)
        
        if not failed_columns:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.PASSED,
                message="All critical columns are 100% populated.",
                details={"columns_checked": CRITICAL_COLUMNS, "stats": null_stats}
            )
        else:
            result = QualityCheckResult(
                check_name=check_name,
                status=CheckStatus.FAILED,
                message=f"Null values found in critical columns: {failed_columns}",
                details={"failed_columns": failed_columns, "stats": null_stats}
            )
        
        self.report.checks.append(result)
        logger.info(f"{check_name}: {result.status.value}")

# ============================================
# Main Entry Point
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Data Quality Checks for Silver Lake")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Target date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--slack-webhook",
        type=str,
        default=None,
        help="Slack webhook URL for alerts"
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Path to write JSON report"
    )
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Data Quality & Observability Check")
    logger.info("=" * 60)
    
    # Run quality checks
    checker = DataQualityChecker(target_date=args.date)
    report = checker.run_all_checks()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("Quality Check Summary")
    logger.info("=" * 60)
    logger.info(f"Target Date: {report.target_date}")
    logger.info(f"Total Checks: {len(report.checks)}")
    logger.info(f"Passed: {len([c for c in report.checks if c.status == CheckStatus.PASSED])}")
    logger.info(f"Failed: {len(report.failed_checks)}")
    logger.info(f"Overall: {'✅ PASSED' if report.passed else '❌ FAILED'}")
    
    # Send alerts if needed
    alert_system = AlertSystem(slack_webhook=args.slack_webhook)
    alert_system.send_alert(report)
    
    # Write JSON report if requested
    if args.output_json:
        with open(args.output_json, 'w') as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Report written to: {args.output_json}")
    
    # Exit with appropriate code
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()

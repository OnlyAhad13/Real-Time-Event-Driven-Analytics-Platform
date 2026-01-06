from fastapi import APIRouter, HTTPException
from app.db.session import db
from typing import List, Dict, Any

router = APIRouter()

@router.get("/daily-active-users")
async def get_dau():
    """Get Daily Active Users trend."""
    query = """
        SELECT 
            activity_date::text as date,
            daily_active_users,
            total_events,
            avg_events_per_user
        FROM public_marts.daily_active_users
        ORDER BY activity_date ASC
        LIMIT 30;
    """
    rows = await db.fetch_all(query)
    return [dict(row) for row in rows]

@router.get("/revenue")
async def get_revenue():
    """Get Top Revenue generating users."""
    query = """
        SELECT 
            user_key,
            total_revenue,
            total_purchases,
            avg_order_value
        FROM public_marts.revenue_per_user
        ORDER BY total_revenue DESC
        LIMIT 50;
    """
    rows = await db.fetch_all(query)
    return [dict(row) for row in rows]

@router.get("/churn-risk")
async def get_churn_risk():
    """Get users with High Churn Risk."""
    query = """
        SELECT 
            user_key,
            days_since_last_activity,
            churn_risk_level
        FROM public_marts.churn_risk
        WHERE churn_risk_level = 'high'
        ORDER BY days_since_last_activity DESC
        LIMIT 50;
    """
    rows = await db.fetch_all(query)
    return [dict(row) for row in rows]

@router.get("/summary")
async def get_summary():
    """Get high-level summary stats."""
    # This is a bit ad-hoc, but useful for the dashboards top cards
    
    # 1. Total Revenue
    revenue_query = "SELECT SUM(total_revenue) as total_revenue FROM public_marts.revenue_per_user"
    rev_row = await db.fetch_one(revenue_query)
    
    # 2. Total Users at Risk
    churn_query = "SELECT COUNT(*) as risk_count FROM public_marts.churn_risk WHERE churn_risk_level = 'high'"
    churn_row = await db.fetch_one(churn_query)
    
    return {
        "total_revenue": rev_row["total_revenue"] if rev_row else 0,
        "high_risk_users": churn_row["risk_count"] if churn_row else 0
    }

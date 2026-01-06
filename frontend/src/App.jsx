
import React, { useEffect, useState, useRef } from 'react';
import { getDAU, getSummary, getLiveSocketUrl } from './services/api';
import KPICard from './components/KPICard';
import RevenueChart from './components/RevenueChart';
import LiveFeed from './components/LiveFeed';
import { DollarSign, Users, AlertTriangle, LayoutDashboard } from 'lucide-react';

function App() {
  const [summary, setSummary] = useState({ total_revenue: 0, high_risk_users: 0 });
  const [dauData, setDauData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [recentEvents, setRecentEvents] = useState([]);
  const activeUserSetRef = useRef(new Set()); // Track unique users for this session
  const [liveUserCount, setLiveUserCount] = useState(0);

  // Initial Data Fetch
  useEffect(() => {
    const fetchData = async () => {
      try {
        const [sum, dau] = await Promise.all([getSummary(), getDAU()]);
        setSummary(sum);
        setDauData(dau);
        // Initialize live count with today's DB value if available
        const todayDAU = dau.length > 0 ? dau[dau.length - 1].daily_active_users : 0;
        setLiveUserCount(todayDAU);
      } catch (error) {
        console.error("Failed to fetch dashboard data:", error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  // WebSocket Connection
  useEffect(() => {
    const ws = new WebSocket(getLiveSocketUrl());

    ws.onmessage = (message) => {
      const event = JSON.parse(message.data);

      // 1. Update Live Feed list
      setRecentEvents(prev => [event, ...prev].slice(0, 10));

      // 2. Update Total Revenue (Optimistic UI)
      if (event.event_type === 'purchase' && event.payload?.total_amount) {
        setSummary(prev => ({
          ...prev,
          total_revenue: Number(prev.total_revenue || 0) + Number(event.payload.total_amount)
        }));
      }

      // 3. Update Active Users (Live Counter)
      if (event.user_id) {
        // Heuristic: If we haven't seen this user in this session, increment
        // Note: In a real app this would drift from DB, but for "Live" feel it works well
        if (!activeUserSetRef.current.has(event.user_id)) {
          activeUserSetRef.current.add(event.user_id);
          setLiveUserCount(prev => prev + 1);

          // Also update the chart's "Today" point dynamically
          setDauData(prevData => {
            const newData = [...prevData];
            if (newData.length > 0) {
              // Update last point (Today)
              newData[newData.length - 1] = {
                ...newData[newData.length - 1],
                daily_active_users: newData[newData.length - 1].daily_active_users + 1
              };
            }
            return newData;
          });
        }
      }
    };

    return () => {
      ws.close();
    };
  }, []); // Empty dependency array as activeUserSetRef is a stable ref

  if (loading) {
    return (
      <div className="flex h-screen w-full items-center justify-center bg-background text-foreground">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background text-foreground p-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <LayoutDashboard className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Analytics Dashboard</h1>
            <p className="text-muted-foreground">Real-Time Insights & Overview</p>
          </div>
        </div>
        <div className="text-sm bg-accent/50 px-3 py-1 rounded-full border flex items-center gap-2">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
          </span>
          Status: <span className="text-green-500 font-medium">System Online</span>
        </div>
      </div>

      {/* KPI Cloud */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
        <KPICard
          title="Total Revenue"
          value={`$${Number(summary.total_revenue).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
          icon={DollarSign}
          trend="Live Updates"
          className="transition-all duration-300"
        />
        <KPICard
          title="Active Users (Today)"
          value={liveUserCount.toLocaleString()}
          icon={Users}
          trend="+ Live Activity"
          className="transition-all duration-300"
        />
        <KPICard
          title="High Churn Risk"
          value={summary.high_risk_users}
          icon={AlertTriangle}
          trend="Needs Attention"
          className="border-red-200 dark:border-red-900"
        />
        <KPICard
          title="System Health"
          value="100%"
          trend="Fully Operational"
          className="border-green-200 dark:border-green-900"
        />
      </div>

      {/* Main Content Grid */}
      <div className="grid gap-4 md:grid-cols-7">

        {/* Main Chart Area */}
        <div className="col-span-4">
          <RevenueChart data={dauData} />
        </div>

        {/* Live Feed Sidebar */}
        <div className="col-span-3">
          <LiveFeed events={recentEvents} />
        </div>
      </div>
    </div>
  );
}

export default App;

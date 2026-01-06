import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const RevenueChart = ({ data }) => {
    return (
        <div className="rounded-xl border bg-card text-card-foreground shadow p-6 max-w-full">
            <div className="flex flex-col space-y-1.5 mb-4">
                <h3 className="font-semibold leading-none tracking-tight">Active Users Trend</h3>
                <p className="text-sm text-muted-foreground">Daily Active Users over the last 30 days</p>
            </div>
            <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} className="stroke-muted" />
                        <XAxis
                            dataKey="date"
                            tickLine={false}
                            axisLine={false}
                            tickMargin={10}
                            fontSize={12}
                            tickFormatter={(value) => value.slice(5)} // Show MM-DD
                        />
                        <YAxis
                            tickLine={false}
                            axisLine={false}
                            tickMargin={10}
                            fontSize={12}
                        />
                        <Tooltip
                            contentStyle={{ backgroundColor: 'hsl(var(--card))', borderRadius: '8px', border: '1px solid hsl(var(--border))' }}
                            itemStyle={{ color: 'hsl(var(--foreground))' }}
                        />
                        <Line
                            type="monotone"
                            dataKey="daily_active_users"
                            stroke="hsl(var(--primary))"
                            strokeWidth={2}
                            dot={false}
                            activeDot={{ r: 4 }}
                        />
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};

export default RevenueChart;

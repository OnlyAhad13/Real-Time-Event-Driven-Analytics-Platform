import React, { useEffect, useState, useRef } from 'react';
import { getLiveSocketUrl } from '../services/api';
import { Activity, ShoppingCart, UserPlus, AlertCircle } from 'lucide-react';

const EventRow = ({ event }) => {
    let Icon = Activity;
    let color = "text-blue-500";

    // Determine icon based on event type
    if (event.event_type === 'purchase') {
        Icon = ShoppingCart;
        color = "text-green-500";
    } else if (event.event_type === 'user_signup') {
        Icon = UserPlus;
        color = "text-purple-500";
    } else if (event.event_type === 'payment_failed') {
        Icon = AlertCircle;
        color = "text-red-500";
    }

    // Format timestamp nicely
    // Backend sends 'timestamp' as ISO string (e.g. "2026-01-06T08:25:49...")
    const time = new Date(event.timestamp).toLocaleTimeString();

    return (
        <div className="flex items-center justify-between py-3 border-b border-border last:border-0">
            <div className="flex items-center gap-3">
                <div className={`p-2 rounded-full bg-accent ${color}`}>
                    <Icon size={16} />
                </div>
                <div>
                    <p className="text-sm font-medium leading-none">{event.event_type}</p>
                    <p className="text-xs text-muted-foreground">{event.user_id?.slice(0, 8)}...</p>
                </div>
            </div>
            <div className="text-right">
                <p className="text-xs font-mono text-muted-foreground">{time}</p>
                {event.payload?.total_amount && (
                    <p className="text-xs font-bold text-green-600">+${event.payload.total_amount}</p>
                )}
            </div>
        </div>
    );
};

const LiveFeed = ({ events = [] }) => {
    return (
        <div className="rounded-xl border bg-card text-card-foreground shadow p-6 h-full flex flex-col">
            <div className="flex flex-col space-y-1.5 mb-4 shrink-0">
                <h3 className="font-semibold leading-none tracking-tight flex items-center gap-2">
                    <span className="relative flex h-3 w-3">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
                        <span className="relative inline-flex rounded-full h-3 w-3 bg-red-500"></span>
                    </span>
                    Live Event Stream
                </h3>
                <p className="text-sm text-muted-foreground">Real-time events from Kafka</p>
            </div>
            <div className="space-y-1 overflow-auto flex-1 pr-1 custom-scrollbar">
                {events.length === 0 ? (
                    <div className="text-center py-8 text-muted-foreground text-sm">Waiting for live events...</div>
                ) : (
                    events.map((event, idx) => (
                        <EventRow key={`${event.event_id}-${idx}`} event={event} />
                    ))
                )}
            </div>
        </div>
    );
};

export default LiveFeed;

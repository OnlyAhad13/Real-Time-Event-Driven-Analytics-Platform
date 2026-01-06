import React from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs) {
    return twMerge(clsx(inputs));
}

const KPICard = ({ title, value, icon: Icon, trend, className }) => {
    return (
        <div className={cn("rounded-xl border bg-card text-card-foreground shadow p-6", className)}>
            <div className="flex flex-row items-center justify-between space-y-0 pb-2">
                <h3 className="tracking-tight text-sm font-medium text-muted-foreground">{title}</h3>
                {Icon && <Icon className="h-4 w-4 text-muted-foreground" />}
            </div>
            <div className="text-2xl font-bold">{value}</div>
            {trend && (
                <p className="text-xs text-muted-foreground mt-1">
                    {trend}
                </p>
            )}
        </div>
    );
};

export default KPICard;

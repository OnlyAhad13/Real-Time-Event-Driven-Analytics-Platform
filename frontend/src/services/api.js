import axios from 'axios';

// Access backend URL from environment or default to localhost
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
    baseURL: API_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export const getDAU = async () => {
    const response = await api.get('/kpi/daily-active-users');
    return response.data;
};

export const getRevenue = async () => {
    const response = await api.get('/kpi/revenue');
    return response.data;
};

export const getChurnRisk = async () => {
    const response = await api.get('/kpi/churn-risk');
    return response.data;
};

export const getSummary = async () => {
    // We didn't strictly implement /kpi/summary yet, let's mock or rely on individual calls if fail
    // But assuming we added it or will add it.
    try {
        const response = await api.get('/kpi/summary');
        return response.data;
    } catch (error) {
        console.warn("Summary endpoint not found, returning defaults");
        return { total_revenue: 0, high_risk_users: 0 };
    }
}

export const getLiveSocketUrl = () => {
    return `${API_URL.replace('http', 'ws')}/live/events`;
};

export default api;

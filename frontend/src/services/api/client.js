import axios from 'axios';
const isBrowser = typeof window !== 'undefined';
const fallbackBaseUrl = import.meta.env.DEV || !isBrowser ? 'http://localhost:8000' : window.location.origin;
const rawBaseUrl = import.meta.env.VITE_API_URL ?? fallbackBaseUrl;
const normalizedBaseUrl = rawBaseUrl.endsWith('/api')
    ? rawBaseUrl
    : `${rawBaseUrl.replace(/\/$/, '')}/api`;
const shouldSkipCamelCase = (url) => {
    if (!url) {
        return false;
    }
    // Preserve payload keys for preview endpoints so column names remain untouched.
    return /\/preview(\?|$)/.test(url);
};
const client = axios.create({
    baseURL: normalizedBaseUrl
});
const isJsonRecord = (value) => (typeof value === 'object' && value !== null && !Array.isArray(value));
// Helper function to convert snake_case keys to camelCase
const toCamelCase = (input) => {
    if (input === null || input === undefined)
        return input;
    if (Array.isArray(input))
        return input.map(toCamelCase);
    if (!isJsonRecord(input))
        return input;
    const result = {};
    Object.keys(input).forEach((key) => {
        const camelKey = key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
        const value = input[key];
        // Preserve constructed data payload keys (they must stay snake_case for field matching)
        if (camelKey === 'payload' && isJsonRecord(value)) {
            result[camelKey] = value;
            return;
        }
        result[camelKey] = toCamelCase(value);
    });
    return result;
};
client.interceptors.request.use((config) => {
    const token = localStorage.getItem('cc_token');
    if (token) {
        config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
});
// Response interceptor to convert snake_case to camelCase
client.interceptors.response.use((response) => {
    if (response.data && !shouldSkipCamelCase(response.config?.url)) {
        response.data = toCamelCase(response.data);
    }
    return response;
});
export default client;

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
// Helper function to convert snake_case keys to camelCase
const toCamelCase = (obj) => {
    if (obj === null || obj === undefined)
        return obj;
    if (Array.isArray(obj))
        return obj.map(toCamelCase);
    if (typeof obj !== 'object')
        return obj;
    const result = {};
    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            const camelKey = key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
            result[camelKey] = toCamelCase(obj[key]);
        }
    }
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

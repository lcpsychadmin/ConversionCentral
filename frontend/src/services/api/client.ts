import axios from 'axios';

const isBrowser = typeof window !== 'undefined';
const fallbackBaseUrl = import.meta.env.DEV || !isBrowser ? 'http://localhost:8000' : window.location.origin;
const rawBaseUrl = import.meta.env.VITE_API_URL ?? fallbackBaseUrl;
const normalizedBaseUrl = rawBaseUrl.endsWith('/api')
  ? rawBaseUrl
  : `${rawBaseUrl.replace(/\/$/, '')}/api`;

const client = axios.create({
  baseURL: normalizedBaseUrl
});

client.interceptors.request.use((config) => {
  const token = localStorage.getItem('cc_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export default client;

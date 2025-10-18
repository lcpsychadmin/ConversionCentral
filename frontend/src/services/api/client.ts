import axios from 'axios';

const rawBaseUrl = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
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

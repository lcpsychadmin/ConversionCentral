import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Box, Button, Paper, TextField, Typography } from '@mui/material';
import { useAuth } from '../context/AuthContext';
const LoginPage = () => {
    const { login } = useAuth();
    const navigate = useNavigate();
    const location = useLocation();
    const [email, setEmail] = useState('admin@conversioncentral.com');
    const from = location.state?.from?.pathname ?? '/';
    const resolveRoles = (address) => {
        const normalized = address.trim().toLowerCase();
        if (!normalized) {
            return ['viewer'];
        }
        const roles = ['viewer'];
    const adminEmails = new Set(['admin@conversioncentral.com']);
        if (adminEmails.has(normalized)) {
            roles.push('admin');
        }
        return Array.from(new Set(roles));
    };
    const handleSubmit = (event) => {
        event.preventDefault();
        const mockUser = {
            id: 'demo-user',
            name: 'Demo User',
            email,
            roles: resolveRoles(email)
        };
        login(mockUser);
        navigate(from, { replace: true });
    };
    return (_jsx(Box, { display: "flex", justifyContent: "center", alignItems: "center", minHeight: "100vh", children: _jsxs(Paper, { elevation: 3, sx: { p: 4, minWidth: 360 }, children: [_jsx(Typography, { variant: "h5", gutterBottom: true, children: "Conversion Central Login" }), _jsx(Typography, { variant: "body2", color: "text.secondary", gutterBottom: true, children: "Enter your email to start a mock session." }), _jsxs(Box, { component: "form", onSubmit: handleSubmit, children: [_jsx(TextField, { fullWidth: true, label: "Email", type: "email", margin: "normal", value: email, onChange: (event) => setEmail(event.target.value), required: true }), _jsx(Button, { type: "submit", variant: "contained", fullWidth: true, sx: { mt: 2 }, children: "Sign In" })] })] }) }));
};
export default LoginPage;

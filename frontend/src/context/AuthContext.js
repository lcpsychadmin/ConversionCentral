import { jsx as _jsx } from "react/jsx-runtime";
import { createContext, useCallback, useContext, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
const AuthContext = createContext(undefined);
export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(null);
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();
    const login = useCallback((info) => {
        setLoading(true);
        setUser(info);
        setLoading(false);
    }, []);
    const logout = useCallback(() => {
        setUser(null);
        navigate('/login');
    }, [navigate]);
    const hasRole = useCallback((...roles) => {
        if (!user)
            return false;
        return roles.some((role) => user.roles.includes(role));
    }, [user]);
    const value = useMemo(() => ({
        user,
        loading,
        login,
        logout,
        hasRole
    }), [user, loading, login, logout, hasRole]);
    return _jsx(AuthContext.Provider, { value: value, children: children });
};
export const useAuth = () => {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};

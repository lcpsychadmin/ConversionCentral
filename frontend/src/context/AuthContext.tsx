import { createContext, ReactNode, useCallback, useContext, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

export type Role = 'admin' | 'approver' | 'data_owner' | 'sme' | 'viewer';

export interface UserInfo {
  id: string;
  name: string;
  email: string;
  roles: Role[];
}

interface AuthContextValue {
  user: UserInfo | null;
  loading: boolean;
  login: (user: UserInfo) => void;
  logout: () => void;
  hasRole: (...roles: Role[]) => boolean;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const [user, setUser] = useState<UserInfo | null>(null);
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const login = useCallback((info: UserInfo) => {
    setLoading(true);
    setUser(info);
    setLoading(false);
  }, []);

  const logout = useCallback(() => {
    setUser(null);
    navigate('/login');
  }, [navigate]);

  const hasRole = useCallback((...roles: Role[]) => {
    if (!user) return false;
    return roles.some((role) => user.roles.includes(role));
  }, [user]);

  const value = useMemo(
    () => ({
      user,
      loading,
      login,
      logout,
      hasRole
    }),
    [user, loading, login, logout, hasRole]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

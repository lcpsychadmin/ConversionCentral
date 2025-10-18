import { Navigate, useLocation } from 'react-router-dom';
import { ReactNode } from 'react';

import { useAuth, Role } from '../../context/AuthContext';

interface ProtectedRouteProps {
  children: ReactNode;
  roles?: Role[];
}

export const ProtectedRoute = ({ children, roles }: ProtectedRouteProps) => {
  const { user, loading, hasRole } = useAuth();
  const location = useLocation();

  if (loading) {
    return null;
  }

  if (!user) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  if (roles && !hasRole(...roles)) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};

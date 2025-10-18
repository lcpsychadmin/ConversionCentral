import { useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Box, Button, Paper, TextField, Typography } from '@mui/material';

import { useAuth, UserInfo } from '../context/AuthContext';

const LoginPage = () => {
  const { login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [email, setEmail] = useState('');
  const from = (location.state as { from?: { pathname?: string } })?.from?.pathname ?? '/';

  const resolveRoles = (address: string): UserInfo['roles'] => {
    const normalized = address.trim().toLowerCase();
    if (!normalized) {
      return ['viewer'];
    }

    const roles: UserInfo['roles'] = ['viewer'];
    const adminEmails = new Set(['j.wes.collins@outlook.com']);

    if (adminEmails.has(normalized)) {
      roles.push('admin');
    }

    return Array.from(new Set(roles));
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const mockUser: UserInfo = {
      id: 'demo-user',
      name: 'Demo User',
      email,
      roles: resolveRoles(email)
    };
    login(mockUser);
    navigate(from, { replace: true });
  };

  return (
    <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
      <Paper elevation={3} sx={{ p: 4, minWidth: 360 }}>
        <Typography variant="h5" gutterBottom>
          Conversion Central Login
        </Typography>
        <Typography variant="body2" color="text.secondary" gutterBottom>
          Enter your email to start a mock session.
        </Typography>
        <Box component="form" onSubmit={handleSubmit}>
          <TextField
            fullWidth
            label="Email"
            type="email"
            margin="normal"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            required
          />
          <Button type="submit" variant="contained" fullWidth sx={{ mt: 2 }}>
            Sign In
          </Button>
        </Box>
      </Paper>
    </Box>
  );
};

export default LoginPage;

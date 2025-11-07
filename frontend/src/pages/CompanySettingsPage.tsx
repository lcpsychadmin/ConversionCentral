import { ChangeEvent, FormEvent, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Paper,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useCompanySettings } from '../hooks/useCompanySettings';
import { useAuth } from '../context/AuthContext';

const MAX_LOGO_SIZE_BYTES = 350_000;

const CompanySettingsPage = () => {
  const theme = useTheme();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const { settingsQuery, updateSettings, updating } = useCompanySettings();
  const { data: settings, isLoading, isFetching, isError, error } = settingsQuery;

  const [siteTitle, setSiteTitle] = useState('');
  const [logoDataUrl, setLogoDataUrl] = useState<string | null>(null);
  const [logoError, setLogoError] = useState<string | null>(null);

  useEffect(() => {
    if (!settings) {
      setSiteTitle('');
      setLogoDataUrl(null);
      return;
    }
    setSiteTitle(settings.siteTitle ?? '');
    setLogoDataUrl(settings.logoDataUrl ?? null);
  }, [settings]);

  const normalizedSiteTitle = useMemo(() => {
    const trimmed = siteTitle.trim();
    return trimmed.length > 0 ? trimmed : null;
  }, [siteTitle]);

  const normalizedLogoDataUrl = logoDataUrl ?? null;
  const originalSiteTitle = settings?.siteTitle ?? null;
  const originalLogo = settings?.logoDataUrl ?? null;

  const hasChanges = settings
    ? normalizedSiteTitle !== originalSiteTitle || normalizedLogoDataUrl !== originalLogo
    : normalizedSiteTitle !== null || normalizedLogoDataUrl !== null;

  const disableInputs = !canManage || updating;

  const errorMessage = isError
    ? error instanceof Error
      ? error.message
      : 'Unable to load company settings.'
    : null;

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    if (!file.type.startsWith('image/')) {
      setLogoError('Please select a valid image file (PNG, JPG, or SVG).');
      return;
    }

    if (file.size > MAX_LOGO_SIZE_BYTES) {
      setLogoError('Logo exceeds the 350 KB size limit. Please choose a smaller image.');
      return;
    }

    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result;
      if (typeof result === 'string') {
        setLogoDataUrl(result);
        setLogoError(null);
      }
    };
    reader.onerror = () => {
      setLogoError('Unable to read the selected file. Please try a different image.');
    };
    reader.readAsDataURL(file);
  };

  const handleClearLogo = () => {
    setLogoDataUrl(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleReset = () => {
    setSiteTitle(settings?.siteTitle ?? '');
    setLogoDataUrl(settings?.logoDataUrl ?? null);
    setLogoError(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canManage || (!hasChanges && settings)) {
      return;
    }

    await updateSettings({
      siteTitle: normalizedSiteTitle,
      logoDataUrl: normalizedLogoDataUrl
    });
  };

  if (isLoading && !settings) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
          borderBottom: `3px solid ${theme.palette.primary.main}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
        }}
      >
        <Typography
          variant="h4"
          gutterBottom
          sx={{ color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }}
        >
          Company Settings
        </Typography>
        <Typography
          variant="body2"
          sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}
        >
          Update the site title and upload a logo to customize the Conversion Central experience.
        </Typography>
      </Box>

      {errorMessage && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {errorMessage}
        </Alert>
      )}

      {!canManage && (
        <Alert severity="info" sx={{ mb: 3 }}>
          You have read-only access to company branding. Contact an administrator to make changes.
        </Alert>
      )}

      {logoError && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          {logoError}
        </Alert>
      )}

      <Paper
        component="form"
        elevation={3}
        onSubmit={handleSubmit}
        sx={{ p: 3, mb: 4, maxWidth: 640 }}
      >
        <Stack spacing={3}>
          <TextField
            label="Site Title"
            value={siteTitle}
            onChange={(event) => setSiteTitle(event.target.value)}
            disabled={disableInputs}
            fullWidth
            placeholder="Conversion Central"
            helperText="Displayed in the navigation bar across the application."
            InputLabelProps={{ shrink: siteTitle.trim().length > 0 }}
          />

          <Stack spacing={2}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Company Logo
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Upload a PNG, JPG, or SVG image up to 350 KB. The image is automatically scaled to fit next to the site title.
            </Typography>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/png,image/jpeg,image/svg+xml"
              style={{ display: 'none' }}
              onChange={handleFileChange}
              disabled={disableInputs}
            />
            <Stack direction="row" spacing={2} alignItems="center">
              <Button
                variant="outlined"
                onClick={() => fileInputRef.current?.click()}
                disabled={disableInputs}
              >
                Choose Logo
              </Button>
              {logoDataUrl && (
                <Button
                  type="button"
                  variant="text"
                  color="secondary"
                  onClick={handleClearLogo}
                  disabled={disableInputs}
                >
                  Remove Logo
                </Button>
              )}
            </Stack>
            <Box
              sx={{
                height: 96,
                borderRadius: 1,
                border: `1px dashed ${alpha(theme.palette.primary.main, 0.3)}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                backgroundColor: alpha(theme.palette.primary.light, 0.05)
              }}
            >
              {logoDataUrl ? (
                <Box
                  component="img"
                  src={logoDataUrl}
                  alt="Company logo preview"
                  sx={{ maxHeight: 80, maxWidth: '100%', objectFit: 'contain' }}
                />
              ) : (
                <Typography variant="body2" color="text.secondary">
                  No logo selected.
                </Typography>
              )}
            </Box>
          </Stack>

          <Stack direction="row" spacing={2} justifyContent="flex-end" alignItems="center">
            <Button
              type="button"
              variant="outlined"
              onClick={handleReset}
              disabled={disableInputs || (!settings && !siteTitle && !logoDataUrl)}
            >
              Reset
            </Button>
            <Button
              type="submit"
              variant="contained"
              disabled={!canManage || updating || !hasChanges}
            >
              {updating || isFetching ? 'Saving...' : 'Save Changes'}
            </Button>
          </Stack>
        </Stack>
      </Paper>
    </Box>
  );
};

export default CompanySettingsPage;

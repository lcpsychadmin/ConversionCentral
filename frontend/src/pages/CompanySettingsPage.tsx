import { ChangeEvent, FormEvent, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControlLabel,
  Paper,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';

import { useCompanySettings } from '../hooks/useCompanySettings';
import { useAuth } from '../context/AuthContext';
import { DEFAULT_ACCENT_COLOR, DEFAULT_THEME_MODE } from '../theme/theme';
import PageHeader from '../components/common/PageHeader';

const MAX_LOGO_SIZE_BYTES = 350_000;
const ALLOWED_LOGO_MIME_TYPES = new Set(['image/png', 'image/svg+xml', 'image/webp']);

const isAllowedLogoFile = (file: File) => {
  const normalizedType = file.type?.toLowerCase();
  if (normalizedType && ALLOWED_LOGO_MIME_TYPES.has(normalizedType)) {
    return true;
  }

  const extensionMatch = file.name.toLowerCase().match(/\.(png|svg|webp)$/);
  return Boolean(extensionMatch);
};

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
  const [themeMode, setThemeMode] = useState<'light' | 'dark'>(DEFAULT_THEME_MODE);
  const [accentColor, setAccentColor] = useState<string>(DEFAULT_ACCENT_COLOR);

  useEffect(() => {
    if (!settings) {
      setSiteTitle('');
      setLogoDataUrl(null);
      setThemeMode(DEFAULT_THEME_MODE);
      setAccentColor(DEFAULT_ACCENT_COLOR);
      return;
    }
    setSiteTitle(settings.siteTitle ?? '');
    setLogoDataUrl(settings.logoDataUrl ?? null);
    setThemeMode(settings.themeMode ?? DEFAULT_THEME_MODE);
    setAccentColor(settings.accentColor ?? DEFAULT_ACCENT_COLOR);
  }, [settings]);

  const normalizedSiteTitle = useMemo(() => {
    const trimmed = siteTitle.trim();
    return trimmed.length > 0 ? trimmed : null;
  }, [siteTitle]);

  const normalizedLogoDataUrl = logoDataUrl ?? null;
  const originalSiteTitle = settings?.siteTitle ?? null;
  const originalLogo = settings?.logoDataUrl ?? null;
  const originalThemeMode = settings?.themeMode ?? DEFAULT_THEME_MODE;
  const originalAccentColor = (settings?.accentColor ?? DEFAULT_ACCENT_COLOR).toLowerCase();
  const normalizedAccentColor = accentColor.toLowerCase();

  const hasChanges = settings
    ? normalizedSiteTitle !== originalSiteTitle ||
      normalizedLogoDataUrl !== originalLogo ||
      themeMode !== originalThemeMode ||
      normalizedAccentColor !== originalAccentColor
    : normalizedSiteTitle !== null ||
      normalizedLogoDataUrl !== null ||
      themeMode !== DEFAULT_THEME_MODE ||
      normalizedAccentColor !== DEFAULT_ACCENT_COLOR.toLowerCase();

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

    if (!isAllowedLogoFile(file)) {
      setLogoError('Please select a PNG, SVG, or WebP image to preserve transparency.');
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
    setThemeMode(settings?.themeMode ?? DEFAULT_THEME_MODE);
    setAccentColor(settings?.accentColor ?? DEFAULT_ACCENT_COLOR);
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
      logoDataUrl: normalizedLogoDataUrl,
      themeMode,
      accentColor: normalizedAccentColor
    });
  };

  const handleThemeToggle = (event: ChangeEvent<HTMLInputElement>) => {
    setThemeMode(event.target.checked ? 'dark' : 'light');
  };

  const handleAccentColorChange = (event: ChangeEvent<HTMLInputElement>) => {
    setAccentColor(event.target.value.toLowerCase());
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
      <PageHeader
        title="Company Settings"
        subtitle="Update the site title and upload a logo to customize the Conversion Central experience."
      />

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
              Upload a PNG, SVG, or WebP image up to 350 KB. The image is automatically scaled to fit next to the site title.
            </Typography>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/png,image/svg+xml,image/webp"
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
                backgroundColor: 'transparent'
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

          <Stack spacing={2}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Theme
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Control the default appearance for all users. The accent color updates primary navigation elements and buttons.
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={themeMode === 'dark'}
                  onChange={handleThemeToggle}
                  disabled={disableInputs}
                  color="primary"
                />
              }
              label="Enable dark mode"
            />
            <TextField
              label="Accent Color"
              type="color"
              value={accentColor}
              onChange={handleAccentColorChange}
              disabled={disableInputs}
              InputLabelProps={{ shrink: true }}
              sx={{ width: 160 }}
              helperText="Pick a 6-digit hex color to match your brand."
            />
          </Stack>

          <Stack direction="row" spacing={2} justifyContent="flex-end" alignItems="center">
            <Button
              type="button"
              variant="outlined"
              onClick={handleReset}
              disabled={
                disableInputs ||
                (!settings &&
                  !siteTitle &&
                  !logoDataUrl &&
                  themeMode === DEFAULT_THEME_MODE &&
                  normalizedAccentColor === DEFAULT_ACCENT_COLOR.toLowerCase())
              }
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

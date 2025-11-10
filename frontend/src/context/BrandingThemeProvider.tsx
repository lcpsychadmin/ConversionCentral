import React, { createContext, useContext, useMemo } from 'react';
import { CssBaseline, GlobalStyles, ThemeProvider } from '@mui/material';
import { useQuery } from 'react-query';

import {
  COMPANY_SETTINGS_QUERY_KEY,
  fetchCompanySettings
} from '../services/applicationSettingsService';
import { buildTheme, DEFAULT_ACCENT_COLOR, DEFAULT_THEME_MODE } from '../theme/theme';
import { CompanySettings } from '../types/data';

type BrandingSettingsContextValue = {
  settings: CompanySettings | undefined;
  isLoading: boolean;
  error: unknown;
};

const BrandingSettingsContext = createContext<BrandingSettingsContextValue | undefined>(undefined);

export const BrandingThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { data, isLoading, error } = useQuery(COMPANY_SETTINGS_QUERY_KEY, fetchCompanySettings, {
    staleTime: 5 * 60 * 1000
  });

  const themeMode = data?.themeMode ?? DEFAULT_THEME_MODE;
  const accentColor = data?.accentColor ?? DEFAULT_ACCENT_COLOR;

  const theme = useMemo(() => buildTheme({ mode: themeMode, accentColor }), [themeMode, accentColor]);

  const contextValue = useMemo<BrandingSettingsContextValue>(
    () => ({ settings: data, isLoading, error }),
    [data, error, isLoading]
  );

  return (
    <BrandingSettingsContext.Provider value={contextValue}>
      <ThemeProvider theme={theme}>
        <GlobalStyles
          styles={{
            "input[class^='ag-'][type='number']:not(.ag-number-field-input-stepper)": {
              appearance: 'textfield',
              MozAppearance: 'textfield'
            }
          }}
        />
        <CssBaseline />
        {children}
      </ThemeProvider>
    </BrandingSettingsContext.Provider>
  );
};

export const useBrandingSettings = (): BrandingSettingsContextValue => {
  const context = useContext(BrandingSettingsContext);
  if (!context) {
    throw new Error('useBrandingSettings must be used within a BrandingThemeProvider');
  }
  return context;
};

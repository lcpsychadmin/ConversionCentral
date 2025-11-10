import { Theme, alpha } from '@mui/material/styles';

interface SurfaceOptions {
  shadow?: 'subtle' | 'raised' | 'none';
}

const getShadow = (theme: Theme, variant: SurfaceOptions['shadow']): string | undefined => {
  if (variant === 'none') {
    return undefined;
  }

  const isDark = theme.palette.mode === 'dark';
  const strength = variant === 'raised' ? (isDark ? 0.45 : 0.18) : isDark ? 0.35 : 0.12;
  const offset = variant === 'raised' ? '0 14px 36px' : '0 10px 26px';
  return `${offset} ${alpha(theme.palette.common.black, strength)}`;
};

export const getSectionSurface = (theme: Theme, options?: SurfaceOptions) => {
  const isDark = theme.palette.mode === 'dark';
  const shadow = getShadow(theme, options?.shadow ?? 'subtle');

  return {
    background: isDark
      ? `linear-gradient(205deg, ${alpha(theme.palette.primary.main, 0.28)} 0%, ${alpha(theme.palette.background.paper, 0.94)} 100%)`
      : `linear-gradient(180deg, ${alpha(theme.palette.common.black, 0.05)} 0%, ${alpha(theme.palette.common.white, 0.98)} 100%)`,
    border: `1px solid ${alpha(isDark ? theme.palette.primary.main : theme.palette.common.black, isDark ? 0.4 : 0.08)}`,
    boxShadow: shadow
  } as const;
};

export const getPanelSurface = (theme: Theme, options?: SurfaceOptions) => {
  const isDark = theme.palette.mode === 'dark';
  const shadow = getShadow(theme, options?.shadow ?? 'subtle');

  return {
    background: isDark
      ? alpha(theme.palette.background.paper, 0.96)
      : theme.palette.common.white,
    border: `1px solid ${alpha(isDark ? theme.palette.primary.main : theme.palette.common.black, isDark ? 0.35 : 0.08)}`,
    boxShadow: shadow
  } as const;
};

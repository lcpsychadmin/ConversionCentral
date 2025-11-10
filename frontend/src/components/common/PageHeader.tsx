import { Box, Stack, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { ReactNode } from 'react';

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  disableShadow?: boolean;
}

const PageHeader = ({ title, subtitle, actions, disableShadow = false }: PageHeaderProps) => {
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';

  const background = isDarkMode
    ? `linear-gradient(205deg, ${alpha(theme.palette.primary.main, 0.42)} 0%, ${alpha(theme.palette.background.paper, 0.92)} 100%)`
    : `linear-gradient(130deg, ${alpha(theme.palette.primary.main, 0.16)} 0%, ${alpha(theme.palette.primary.main, 0.06)} 100%)`;

  const borderColor = alpha(theme.palette.primary.main, isDarkMode ? 0.9 : 1);
  const titleColor = isDarkMode ? theme.palette.common.white : theme.palette.primary.dark;
  const subtitleColor = isDarkMode ? alpha(theme.palette.common.white, 0.82) : alpha(theme.palette.primary.dark, 0.82);
  const shadow = disableShadow
    ? 'none'
    : `0 8px 22px ${alpha(theme.palette.primary.main, isDarkMode ? 0.55 : 0.18)}`;

  const stackDirection = { xs: 'column', sm: actions ? 'row' : 'column' } as const;
  const stackAlignment = actions
    ? { xs: 'flex-start', sm: 'center' }
    : { xs: 'flex-start', sm: 'flex-start' };

  return (
    <Box
      sx={{
        background,
        borderBottom: `3px solid ${borderColor}`,
        borderRadius: { xs: 2, md: 3 },
        p: { xs: 2.5, md: 3 },
        mb: 3,
        boxShadow: shadow
      }}
    >
      <Stack direction={stackDirection} spacing={{ xs: 2, sm: 3 }} alignItems={stackAlignment} justifyContent="space-between">
        <Box>
          <Typography
            variant="h4"
            sx={{ color: titleColor, fontWeight: 800, fontSize: { xs: '1.6rem', md: '1.75rem' }, mb: subtitle ? 1 : 0 }}
          >
            {title}
          </Typography>
          {subtitle && (
            <Typography variant="body2" sx={{ color: subtitleColor, fontSize: '0.95rem' }}>
              {subtitle}
            </Typography>
          )}
        </Box>
        {actions && (
          <Box
            sx={{
              alignSelf: { xs: 'stretch', sm: 'center' },
              display: 'flex',
              flexWrap: 'wrap',
              gap: 1.5,
              justifyContent: { xs: 'flex-start', sm: 'flex-end' }
            }}
          >
            {actions}
          </Box>
        )}
      </Stack>
    </Box>
  );
};

export default PageHeader;

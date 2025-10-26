import { useMemo, useState, type DragEvent, type MouseEvent } from 'react';
import { alpha, useTheme } from '@mui/material/styles';
import { Box, Checkbox, Divider, IconButton, Stack, Tooltip, Typography } from '@mui/material';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked';
import { Handle, NodeProps, Position } from 'reactflow';
import { NodeResizer } from '@reactflow/node-resizer';
import '@reactflow/node-resizer/dist/style.css';

interface ReportTableNodeData {
  tableId: string;
  label: string;
  subtitle?: string | null;
  meta?: string;
  fields: Array<{
    id: string;
    name: string;
    type?: string | null;
    description?: string | null;
  }>;
  allowSelection?: boolean;
  selectedFieldIds?: string[];
  onFieldToggle?: (fieldId: string) => void;
  onFieldAdd?: (fieldId: string) => void;
  onFieldDragStart?: (payload: { tableId: string; fieldId: string }) => void;
  onFieldDragEnd?: () => void;
  onFieldJoin?: (
    source: { tableId: string; fieldId: string } | null,
    target: { tableId: string; fieldId: string }
  ) => void;
  onRemoveTable?: (tableId: string) => void;
}

export const REPORTING_FIELD_DRAG_TYPE = 'application/reporting-field';

const ReportTableNode = ({ data, selected }: NodeProps<ReportTableNodeData>) => {
  const theme = useTheme();
  const allowSelection = data.allowSelection !== false;
  const selectedFieldSet = useMemo(() => new Set(data.selectedFieldIds ?? []), [data.selectedFieldIds]);
  const [activeDropFieldId, setActiveDropFieldId] = useState<string | null>(null);

  const handleToggle = (fieldId: string) => {
    if (!allowSelection) {
      return;
    }
    data.onFieldToggle?.(fieldId);
  };

  const handleFieldDragOver = (event: DragEvent<HTMLDivElement>, targetFieldId: string) => {
    if (!data.onFieldJoin) {
      return;
    }

  event.preventDefault();
  event.dataTransfer.dropEffect = 'copy';
    setActiveDropFieldId(targetFieldId);
  };

  const handleFieldDragLeave = () => {
    setActiveDropFieldId(null);
  };

  const handleFieldDrop = (event: DragEvent<HTMLDivElement>, targetFieldId: string) => {
    if (!data.onFieldJoin) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    setActiveDropFieldId(null);

    const raw = event.dataTransfer.getData(REPORTING_FIELD_DRAG_TYPE);
    let payload: { tableId: string; fieldId: string } | null = null;

    try {
      if (raw) {
        const parsed = JSON.parse(raw) as { tableId: string; fieldId: string };
        if (parsed?.fieldId && parsed?.tableId) {
          payload = parsed;
        }
      }
    } catch (error) {
      // Ignore malformed payloads
    }

    data.onFieldJoin?.(payload, { tableId: data.tableId, fieldId: targetFieldId });
    data.onFieldDragEnd?.();
  };

  const handleDragStart = (event: DragEvent<HTMLDivElement>, fieldId: string) => {
    event.dataTransfer.setData(
      REPORTING_FIELD_DRAG_TYPE,
      JSON.stringify({ tableId: data.tableId, fieldId })
    );
    event.dataTransfer.effectAllowed = 'copyMove';
    data.onFieldDragStart?.({ tableId: data.tableId, fieldId });
  };

  const handleDragEnd = () => {
    data.onFieldDragEnd?.();
    setActiveDropFieldId(null);
  };

  const handleRemove = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    data.onRemoveTable?.(data.tableId);
  };

  return (
    <Box
      sx={{
        position: 'relative',
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        height: '100%',
        borderRadius: 2,
        border: `1px solid ${alpha(theme.palette.primary.main, selected ? 0.75 : 0.35)}`,
        boxShadow: selected
          ? `0 12px 28px ${alpha(theme.palette.primary.main, 0.22)}`
          : `0 6px 18px ${alpha(theme.palette.grey[700], 0.12)}`,
        backgroundColor: theme.palette.background.paper,
        minWidth: 220,
        minHeight: 200,
        cursor: 'grab',
        overflow: 'hidden',
        '&:active': {
          cursor: 'grabbing'
        }
      }}
    >
      <NodeResizer
        color={alpha(theme.palette.primary.main, 0.8)}
        isVisible={selected}
        minWidth={220}
        minHeight={200}
        handleStyle={{
          width: 14,
          height: 14,
          borderRadius: 4,
          border: `1px solid ${alpha(theme.palette.primary.dark, 0.65)}`,
          background: alpha(theme.palette.background.paper, 0.95)
        }}
        lineStyle={{
          borderWidth: 1,
          borderStyle: 'dashed',
          borderColor: alpha(theme.palette.primary.main, 0.6)
        }}
      />
      <Box
        sx={{
          background: `linear-gradient(90deg, ${alpha(theme.palette.primary.main, 0.92)} 0%, ${alpha(theme.palette.primary.dark, 0.85)} 100%)`,
          borderBottom: `1px solid ${alpha(theme.palette.primary.dark, 0.55)}`,
          px: 1.6,
          py: 1.2,
          flexShrink: 0
        }}
      >
        <Stack direction="row" alignItems="flex-start" spacing={1}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography
              variant="subtitle2"
              sx={{ fontWeight: 700, color: theme.palette.common.white, letterSpacing: 0.2 }}
            >
              {data.label}
            </Typography>
          </Box>
          {data.onRemoveTable && (
            <Tooltip title="Remove table">
              <IconButton
                size="small"
                onClick={handleRemove}
                sx={{
                  color: alpha(theme.palette.common.white, 0.9),
                  '&:hover': {
                    color: theme.palette.error.light,
                    backgroundColor: alpha(theme.palette.error.light, 0.12)
                  }
                }}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          )}
        </Stack>
      </Box>

      <Divider sx={{ borderColor: alpha(theme.palette.primary.main, 0.22), flexShrink: 0 }} />

      <Box
        sx={{
          px: 1.4,
          py: 1.1,
          flex: 1,
          overflowY: 'auto',
          backgroundColor: alpha(theme.palette.background.default, 0.85)
        }}
      >
        {data.fields.length === 0 ? (
          <Typography variant="caption" color="text.secondary">
            No fields available
          </Typography>
        ) : (
          <Stack spacing={0.6}>
            {data.fields.map((field) => {
              const isSelected = selectedFieldSet.has(field.id);
              const isDropTarget = activeDropFieldId === field.id;

              return (
                <Box
                  key={field.id}
                  draggable
                  onDragStart={(event) => handleDragStart(event, field.id)}
                  onClick={
                    allowSelection
                      ? (event) => {
                          event.stopPropagation();
                          handleToggle(field.id);
                        }
                      : undefined
                  }
                  onDragOver={(event) => handleFieldDragOver(event, field.id)}
                  onDragLeave={handleFieldDragLeave}
                  onDrop={(event) => handleFieldDrop(event, field.id)}
                  onDragEnd={handleDragEnd}
                  sx={{
                    position: 'relative',
                    borderRadius: 1,
                    border: `1px solid ${alpha(theme.palette.primary.main, isDropTarget || isSelected ? 0.65 : 0.18)}`,
                    backgroundColor: isDropTarget
                      ? alpha(theme.palette.secondary.light, 0.28)
                      : isSelected
                        ? alpha(theme.palette.primary.light, 0.28)
                        : alpha(theme.palette.common.white, 0.9),
                    boxShadow: isDropTarget
                      ? `0 6px 18px ${alpha(theme.palette.secondary.main, 0.28)}`
                      : isSelected
                        ? `0 4px 14px ${alpha(theme.palette.primary.main, 0.18)}`
                        : `0 2px 8px ${alpha(theme.palette.grey[700], 0.08)}`,
                    padding: '6px 10px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 0.75,
                    cursor: 'pointer',
                    transition: 'all 150ms ease',
                    '&:hover': {
                      borderColor: alpha(theme.palette.primary.main, 0.55),
                      backgroundColor: alpha(theme.palette.primary.light, 0.18)
                    }
                  }}
                >
                  <Handle
                    type="target"
                    position={Position.Left}
                    id={`target:${field.id}`}
                    style={{
                      position: 'absolute',
                      top: '50%',
                      left: -10,
                      transform: 'translateY(-50%)',
                      background: theme.palette.secondary.main,
                      border: 'none',
                      width: 10,
                      height: 10,
                      borderRadius: '50%',
                      boxShadow: `0 0 0 2px ${theme.palette.background.paper}`
                    }}
                  />
                  {allowSelection ? (
                    <Checkbox
                      size="small"
                      checked={isSelected}
                      icon={<RadioButtonUncheckedIcon fontSize="small" />}
                      checkedIcon={<CheckCircleIcon fontSize="small" />}
                      onClick={(event) => event.stopPropagation()}
                      onChange={(event) => {
                        event.stopPropagation();
                        handleToggle(field.id);
                      }}
                      sx={{
                        p: 0.25,
                        color: alpha(theme.palette.text.secondary, 0.6),
                        '&.Mui-checked': {
                          color: theme.palette.primary.main
                        }
                      }}
                    />
                  ) : null}
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography
                      variant="body2"
                      sx={{ fontWeight: 600, fontSize: 13 }}
                      noWrap
                    >
                      {field.name}
                    </Typography>
                    {(field.type || field.description) && (
                      <Typography variant="caption" color="text.secondary" noWrap>
                        {[field.type, field.description].filter(Boolean).join(' · ')}
                      </Typography>
                    )}
                  </Box>
                  <Handle
                    type="source"
                    position={Position.Right}
                    id={`source:${field.id}`}
                    style={{
                      position: 'absolute',
                      top: '50%',
                      right: -10,
                      transform: 'translateY(-50%)',
                      background: theme.palette.primary.main,
                      border: 'none',
                      width: 10,
                      height: 10,
                      borderRadius: '50%',
                      boxShadow: `0 0 0 2px ${theme.palette.background.paper}`
                    }}
                  />
                </Box>
              );
            })}
          </Stack>
        )}
      </Box>
    </Box>
  );
};

export type { ReportTableNodeData };
export default ReportTableNode;

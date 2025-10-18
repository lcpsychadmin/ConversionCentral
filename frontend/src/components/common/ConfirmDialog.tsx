import { LoadingButton } from '@mui/lab';
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle
} from '@mui/material';

interface ConfirmDialogProps {
  open: boolean;
  title: string;
  description?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  confirmColor?: 'inherit' | 'primary' | 'secondary' | 'success' | 'error' | 'info' | 'warning';
  onClose: () => void;
  onConfirm: () => void;
  loading?: boolean;
  confirmDisabled?: boolean;
}

const ConfirmDialog = ({
  open,
  title,
  description,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  confirmColor = 'error',
  onClose,
  onConfirm,
  loading = false,
  confirmDisabled = false
}: ConfirmDialogProps) => (
  <Dialog open={open} onClose={onClose} maxWidth="xs" fullWidth>
    <DialogTitle>{title}</DialogTitle>
    {description && (
      <DialogContent>
        <DialogContentText>{description}</DialogContentText>
      </DialogContent>
    )}
    <DialogActions>
      <Button onClick={onClose} disabled={loading}>
        {cancelLabel}
      </Button>
      {loading ? (
        <LoadingButton loading color={confirmColor} variant="contained">
          {confirmLabel}
        </LoadingButton>
      ) : (
        <Button
          color={confirmColor}
          onClick={onConfirm}
          variant="contained"
          disabled={confirmDisabled}
        >
          {confirmLabel}
        </Button>
      )}
    </DialogActions>
  </Dialog>
);

export default ConfirmDialog;

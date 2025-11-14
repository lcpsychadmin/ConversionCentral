import create from 'zustand';

interface DesignerStore {
  hasUnsaved: boolean;
  setHasUnsaved: (v: boolean) => void;
}

export const useDesignerStore = create<DesignerStore>((set) => ({
  hasUnsaved: false,
  setHasUnsaved: (v: boolean) => set({ hasUnsaved: v })
}));

export default useDesignerStore;

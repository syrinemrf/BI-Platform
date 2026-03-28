import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { Dataset, ETLJob, TableInfo, KPI } from '../types';

interface DataState {
  datasets: Dataset[];
  selectedDataset: Dataset | null;
  currentETLJob: ETLJob | null;
  tables: TableInfo[];
  kpis: KPI[];
  loading: {
    datasets: boolean;
    etl: boolean;
    tables: boolean;
    kpis: boolean;
  };
}

const initialState: DataState = {
  datasets: [],
  selectedDataset: null,
  currentETLJob: null,
  tables: [],
  kpis: [],
  loading: {
    datasets: false,
    etl: false,
    tables: false,
    kpis: false,
  },
};

const dataSlice = createSlice({
  name: 'data',
  initialState,
  reducers: {
    setDatasets: (state, action: PayloadAction<Dataset[]>) => {
      state.datasets = action.payload;
    },
    addDataset: (state, action: PayloadAction<Dataset>) => {
      state.datasets.unshift(action.payload);
    },
    removeDataset: (state, action: PayloadAction<number>) => {
      state.datasets = state.datasets.filter((d) => d.id !== action.payload);
      if (state.selectedDataset?.id === action.payload) {
        state.selectedDataset = null;
      }
    },
    setSelectedDataset: (state, action: PayloadAction<Dataset | null>) => {
      state.selectedDataset = action.payload;
    },
    setCurrentETLJob: (state, action: PayloadAction<ETLJob | null>) => {
      state.currentETLJob = action.payload;
    },
    updateETLJob: (state, action: PayloadAction<Partial<ETLJob> & { id: number }>) => {
      if (state.currentETLJob?.id === action.payload.id) {
        state.currentETLJob = { ...state.currentETLJob, ...action.payload };
      }
    },
    setTables: (state, action: PayloadAction<TableInfo[]>) => {
      state.tables = action.payload;
    },
    setKPIs: (state, action: PayloadAction<KPI[]>) => {
      state.kpis = action.payload;
    },
    setLoading: (state, action: PayloadAction<{ key: keyof DataState['loading']; value: boolean }>) => {
      state.loading[action.payload.key] = action.payload.value;
    },
  },
});

export const {
  setDatasets,
  addDataset,
  removeDataset,
  setSelectedDataset,
  setCurrentETLJob,
  updateETLJob,
  setTables,
  setKPIs,
  setLoading,
} = dataSlice.actions;

export default dataSlice.reducer;

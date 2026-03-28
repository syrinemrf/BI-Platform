import { configureStore } from '@reduxjs/toolkit';
import uiReducer from './uiSlice';
import dataReducer from './dataSlice';

export const store = configureStore({
  reducer: {
    ui: uiReducer,
    data: dataReducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

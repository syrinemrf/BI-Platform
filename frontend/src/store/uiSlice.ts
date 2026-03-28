import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { Theme, Language } from '../types';

interface UIState {
  theme: Theme;
  language: Language;
  sidebarOpen: boolean;
  sidebarCollapsed: boolean;
}

const getInitialTheme = (): Theme => {
  const saved = localStorage.getItem('theme');
  if (saved === 'light' || saved === 'dark' || saved === 'system') {
    return saved;
  }
  return 'system';
};

const getInitialLanguage = (): Language => {
  const saved = localStorage.getItem('language');
  if (saved === 'en' || saved === 'fr') {
    return saved;
  }
  return navigator.language.startsWith('fr') ? 'fr' : 'en';
};

const initialState: UIState = {
  theme: getInitialTheme(),
  language: getInitialLanguage(),
  sidebarOpen: true,
  sidebarCollapsed: false,
};

const uiSlice = createSlice({
  name: 'ui',
  initialState,
  reducers: {
    setTheme: (state, action: PayloadAction<Theme>) => {
      state.theme = action.payload;
      localStorage.setItem('theme', action.payload);

      // Apply theme to document
      const root = document.documentElement;
      if (action.payload === 'dark') {
        root.classList.add('dark');
      } else if (action.payload === 'light') {
        root.classList.remove('dark');
      } else {
        // System preference
        if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
          root.classList.add('dark');
        } else {
          root.classList.remove('dark');
        }
      }
    },
    setLanguage: (state, action: PayloadAction<Language>) => {
      state.language = action.payload;
      localStorage.setItem('language', action.payload);
    },
    toggleSidebar: (state) => {
      state.sidebarOpen = !state.sidebarOpen;
    },
    setSidebarOpen: (state, action: PayloadAction<boolean>) => {
      state.sidebarOpen = action.payload;
    },
    toggleSidebarCollapsed: (state) => {
      state.sidebarCollapsed = !state.sidebarCollapsed;
    },
  },
});

export const {
  setTheme,
  setLanguage,
  toggleSidebar,
  setSidebarOpen,
  toggleSidebarCollapsed,
} = uiSlice.actions;

export default uiSlice.reducer;

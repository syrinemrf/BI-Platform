import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { AuthUser } from '../types';

interface AuthState {
  user: AuthUser | null;
  token: string | null;
  isAuthenticated: boolean;
  isGuest: boolean;
  sessionId: string | null;
}

const getStoredAuth = (): { user: AuthUser | null; token: string | null } => {
  try {
    const token = localStorage.getItem('bi_token');
    const userStr = localStorage.getItem('bi_user');
    if (token && userStr) {
      return { token, user: JSON.parse(userStr) };
    }
  } catch {}
  return { token: null, user: null };
};

const getOrCreateSessionId = (): string => {
  let sessionId = sessionStorage.getItem('bi_session_id');
  if (!sessionId) {
    sessionId = 'guest_' + crypto.randomUUID();
    sessionStorage.setItem('bi_session_id', sessionId);
  }
  return sessionId;
};

const stored = getStoredAuth();
const existingSessionId = sessionStorage.getItem('bi_session_id');

const initialState: AuthState = {
  user: stored.user,
  token: stored.token,
  isAuthenticated: !!stored.token,
  isGuest: !stored.token && !!existingSessionId,
  sessionId: !stored.token && existingSessionId ? existingSessionId : null,
};

const authSlice = createSlice({
  name: 'auth',
  initialState,
  reducers: {
    setCredentials: (state, action: PayloadAction<{ user: AuthUser; token: string }>) => {
      state.user = action.payload.user;
      state.token = action.payload.token;
      state.isAuthenticated = true;
      state.isGuest = false;
      state.sessionId = null;
      localStorage.setItem('bi_token', action.payload.token);
      localStorage.setItem('bi_user', JSON.stringify(action.payload.user));
    },
    logout: (state) => {
      state.user = null;
      state.token = null;
      state.isAuthenticated = false;
      state.isGuest = true;
      state.sessionId = getOrCreateSessionId();
      localStorage.removeItem('bi_token');
      localStorage.removeItem('bi_user');
    },
    continueAsGuest: (state) => {
      state.isGuest = true;
      state.isAuthenticated = false;
      state.sessionId = getOrCreateSessionId();
    },
  },
});

export const { setCredentials, logout, continueAsGuest } = authSlice.actions;
export default authSlice.reducer;

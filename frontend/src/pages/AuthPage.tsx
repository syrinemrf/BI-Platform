import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDispatch } from 'react-redux';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import toast from 'react-hot-toast';
import { setCredentials, continueAsGuest } from '../store/authSlice';
import { authApi } from '../services/api';
import LanguageToggle from '../components/common/LanguageToggle';

const AuthPage: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useDispatch();
  const navigate = useNavigate();
  const [isLogin, setIsLogin] = useState(true);
  const [loading, setLoading] = useState(false);

  const [form, setForm] = useState({
    email: '',
    username: '',
    password: '',
    confirmPassword: '',
    full_name: '',
  });

  const [errors, setErrors] = useState<Record<string, string>>({});

  const validate = () => {
    const errs: Record<string, string> = {};
    if (!form.email) errs.email = t('auth.emailRequired');
    else if (!/\S+@\S+\.\S+/.test(form.email)) errs.email = t('auth.emailInvalid');
    if (!form.password) errs.password = t('auth.passwordRequired');
    else if (form.password.length < 6) errs.password = t('auth.passwordMin');
    if (!isLogin) {
      if (!form.username) errs.username = t('auth.usernameRequired');
      else if (form.username.length < 3) errs.username = t('auth.usernameMin');
      if (form.password !== form.confirmPassword) errs.confirmPassword = t('auth.passwordMismatch');
    }
    setErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validate()) return;
    setLoading(true);
    try {
      let response;
      if (isLogin) {
        response = await authApi.login({ email: form.email, password: form.password });
      } else {
        response = await authApi.register({
          email: form.email,
          username: form.username,
          password: form.password,
          full_name: form.full_name || undefined,
        });
      }
      dispatch(setCredentials({ user: response.user, token: response.access_token }));
      toast.success(isLogin ? t('auth.loginSuccess') : t('auth.registerSuccess'));
      navigate('/');
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : t('errors.generic');
      toast.error(message);
    } finally {
      setLoading(false);
    }
  };

  const handleGuest = () => {
    dispatch(continueAsGuest());
    navigate('/');
  };

  const updateField = (field: string, value: string) => {
    setForm((prev) => ({ ...prev, [field]: value }));
    if (errors[field]) setErrors((prev) => ({ ...prev, [field]: '' }));
  };

  return (
    <div className="min-h-screen flex bg-slate-50 dark:bg-slate-900">
      {/* Left panel - Branding */}
      <div className="hidden lg:flex lg:w-1/2 relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-primary-600 via-primary-700 to-primary-900" />
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-20 left-20 w-72 h-72 bg-white rounded-full blur-3xl" />
          <div className="absolute bottom-20 right-20 w-96 h-96 bg-accent-400 rounded-full blur-3xl" />
        </div>
        <div className="relative z-10 flex flex-col justify-center px-16 text-white">
          <div className="flex items-center gap-4 mb-8">
            <div className="h-16 w-16 rounded-2xl bg-white/20 backdrop-blur-sm flex items-center justify-center">
              <span className="text-3xl font-bold">BI</span>
            </div>
            <div>
              <h1 className="text-3xl font-bold">BI Platform</h1>
              <p className="text-primary-200 text-sm">{t('auth.brandTagline')}</p>
            </div>
          </div>
          <h2 className="text-4xl font-bold leading-tight mb-6">
            {t('auth.heroTitle')}
          </h2>
          <p className="text-lg text-primary-100 mb-8 leading-relaxed">
            {t('auth.heroDescription')}
          </p>
          <div className="space-y-4">
            {['auth.feature1', 'auth.feature2', 'auth.feature3', 'auth.feature4'].map((key) => (
              <div key={key} className="flex items-center gap-3">
                <div className="h-8 w-8 rounded-lg bg-white/20 flex items-center justify-center flex-shrink-0">
                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                </div>
                <span className="text-primary-100">{t(key)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Right panel - Form */}
      <div className="flex-1 flex flex-col justify-center items-center px-6 py-12">
        <div className="absolute top-4 right-4">
          <LanguageToggle variant="text" />
        </div>

        <div className="w-full max-w-md">
          {/* Mobile logo */}
          <div className="lg:hidden flex items-center gap-3 mb-8 justify-center">
            <div className="h-12 w-12 rounded-xl bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center shadow-lg">
              <span className="text-white font-bold text-xl">BI</span>
            </div>
            <span className="font-bold text-2xl text-slate-900 dark:text-white">Platform</span>
          </div>

          <div className="text-center mb-8">
            <h2 className="text-2xl font-bold text-slate-900 dark:text-white">
              {isLogin ? t('auth.welcomeBack') : t('auth.createAccount')}
            </h2>
            <p className="mt-2 text-slate-500 dark:text-slate-400">
              {isLogin ? t('auth.loginSubtitle') : t('auth.registerSubtitle')}
            </p>
          </div>

          {/* Tab switch */}
          <div className="flex bg-slate-100 dark:bg-slate-800 rounded-xl p-1 mb-8">
            <button
              onClick={() => setIsLogin(true)}
              className={`flex-1 py-2.5 text-sm font-medium rounded-lg transition-all ${
                isLogin
                  ? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow-sm'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-700'
              }`}
            >
              {t('auth.login')}
            </button>
            <button
              onClick={() => setIsLogin(false)}
              className={`flex-1 py-2.5 text-sm font-medium rounded-lg transition-all ${
                !isLogin
                  ? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow-sm'
                  : 'text-slate-500 dark:text-slate-400 hover:text-slate-700'
              }`}
            >
              {t('auth.signUp')}
            </button>
          </div>

          <AnimatePresence mode="wait">
            <motion.form
              key={isLogin ? 'login' : 'register'}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              transition={{ duration: 0.2 }}
              onSubmit={handleSubmit}
              className="space-y-4"
            >
              {!isLogin && (
                <>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
                      {t('auth.fullName')}
                    </label>
                    <input
                      type="text"
                      value={form.full_name}
                      onChange={(e) => updateField('full_name', e.target.value)}
                      className="w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all"
                      placeholder={t('auth.fullNamePlaceholder')}
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
                      {t('auth.username')} *
                    </label>
                    <input
                      type="text"
                      value={form.username}
                      onChange={(e) => updateField('username', e.target.value)}
                      className={`w-full px-4 py-3 rounded-xl border ${errors.username ? 'border-red-400' : 'border-slate-200 dark:border-slate-700'} bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all`}
                      placeholder={t('auth.usernamePlaceholder')}
                    />
                    {errors.username && <p className="mt-1 text-sm text-red-500">{errors.username}</p>}
                  </div>
                </>
              )}

              <div>
                <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
                  {t('auth.email')} *
                </label>
                <input
                  type="email"
                  value={form.email}
                  onChange={(e) => updateField('email', e.target.value)}
                  className={`w-full px-4 py-3 rounded-xl border ${errors.email ? 'border-red-400' : 'border-slate-200 dark:border-slate-700'} bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all`}
                  placeholder={t('auth.emailPlaceholder')}
                />
                {errors.email && <p className="mt-1 text-sm text-red-500">{errors.email}</p>}
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
                  {t('auth.password')} *
                </label>
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => updateField('password', e.target.value)}
                  className={`w-full px-4 py-3 rounded-xl border ${errors.password ? 'border-red-400' : 'border-slate-200 dark:border-slate-700'} bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all`}
                  placeholder="••••••••"
                />
                {errors.password && <p className="mt-1 text-sm text-red-500">{errors.password}</p>}
              </div>

              {!isLogin && (
                <div>
                  <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
                    {t('auth.confirmPassword')} *
                  </label>
                  <input
                    type="password"
                    value={form.confirmPassword}
                    onChange={(e) => updateField('confirmPassword', e.target.value)}
                    className={`w-full px-4 py-3 rounded-xl border ${errors.confirmPassword ? 'border-red-400' : 'border-slate-200 dark:border-slate-700'} bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all`}
                    placeholder="••••••••"
                  />
                  {errors.confirmPassword && <p className="mt-1 text-sm text-red-500">{errors.confirmPassword}</p>}
                </div>
              )}

              <button
                type="submit"
                disabled={loading}
                className="w-full py-3 px-4 bg-gradient-to-r from-primary-600 to-primary-700 text-white font-semibold rounded-xl hover:from-primary-700 hover:to-primary-800 focus:ring-4 focus:ring-primary-500/25 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-primary-500/25"
              >
                {loading ? (
                  <span className="flex items-center justify-center gap-2">
                    <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                    </svg>
                    {t('common.loading')}
                  </span>
                ) : isLogin ? t('auth.loginButton') : t('auth.signUpButton')}
              </button>
            </motion.form>
          </AnimatePresence>

          {/* Divider */}
          <div className="relative my-8">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-slate-200 dark:border-slate-700" />
            </div>
            <div className="relative flex justify-center text-sm">
              <span className="px-4 bg-slate-50 dark:bg-slate-900 text-slate-500">{t('auth.or')}</span>
            </div>
          </div>

          {/* Guest mode */}
          <button
            onClick={handleGuest}
            className="w-full py-3 px-4 border-2 border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300 font-medium rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-all"
          >
            {t('auth.continueAsGuest')}
          </button>
          <p className="mt-3 text-center text-xs text-slate-400 dark:text-slate-500">
            {t('auth.guestDisclaimer')}
          </p>
        </div>
      </div>
    </div>
  );
};

export default AuthPage;

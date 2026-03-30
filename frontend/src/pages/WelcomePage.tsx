import React from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { useDispatch, useSelector } from 'react-redux';
import { motion } from 'framer-motion';
import {
  CloudArrowUpIcon,
  CpuChipIcon,
  CubeTransparentIcon,
  ChartBarSquareIcon,
  SparklesIcon,
  ShieldCheckIcon,
  ArrowRightIcon,
  UserPlusIcon,
} from '@heroicons/react/24/outline';
import { continueAsGuest } from '../store/authSlice';
import type { RootState } from '../store';
import LanguageToggle from '../components/common/LanguageToggle';

const WelcomePage: React.FC = () => {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const dispatch = useDispatch();
  const { isAuthenticated } = useSelector((state: RootState) => state.auth);

  const handleGetStarted = () => {
    if (!isAuthenticated) {
      dispatch(continueAsGuest());
    }
    navigate('/data-sources');
  };

  const handleSignIn = () => {
    navigate('/auth');
  };

  const steps = [
    {
      icon: CloudArrowUpIcon,
      color: 'from-blue-500 to-cyan-500',
      bg: 'bg-blue-50 dark:bg-blue-900/20',
      title: t('welcome.step1Title'),
      desc: t('welcome.step1Desc'),
    },
    {
      icon: ShieldCheckIcon,
      color: 'from-purple-500 to-pink-500',
      bg: 'bg-purple-50 dark:bg-purple-900/20',
      title: t('welcome.step2Title'),
      desc: t('welcome.step2Desc'),
    },
    {
      icon: CpuChipIcon,
      color: 'from-amber-500 to-orange-500',
      bg: 'bg-amber-50 dark:bg-amber-900/20',
      title: t('welcome.step3Title'),
      desc: t('welcome.step3Desc'),
    },
    {
      icon: CubeTransparentIcon,
      color: 'from-emerald-500 to-green-500',
      bg: 'bg-emerald-50 dark:bg-emerald-900/20',
      title: t('welcome.step4Title'),
      desc: t('welcome.step4Desc'),
    },
    {
      icon: ChartBarSquareIcon,
      color: 'from-indigo-500 to-violet-500',
      bg: 'bg-indigo-50 dark:bg-indigo-900/20',
      title: t('welcome.step5Title'),
      desc: t('welcome.step5Desc'),
    },
    {
      icon: SparklesIcon,
      color: 'from-rose-500 to-pink-500',
      bg: 'bg-rose-50 dark:bg-rose-900/20',
      title: t('welcome.step6Title'),
      desc: t('welcome.step6Desc'),
    },
  ];

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 overflow-hidden">
      {/* Top bar */}
      <div className="absolute top-4 right-4 z-20 flex items-center gap-3">
        <LanguageToggle variant="text" />
        {!isAuthenticated && (
          <button
            onClick={handleSignIn}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-primary-600 dark:text-primary-400 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-all shadow-sm"
          >
            <UserPlusIcon className="h-4 w-4" />
            {t('welcome.signIn')}
          </button>
        )}
      </div>

      {/* Hero Section */}
      <div className="relative px-6 pt-16 pb-12 lg:pt-24 lg:pb-20">
        {/* Background decorations */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none">
          <div className="absolute -top-40 -right-40 w-96 h-96 bg-primary-500/10 rounded-full blur-3xl" />
          <div className="absolute -bottom-40 -left-40 w-96 h-96 bg-accent-500/10 rounded-full blur-3xl" />
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-gradient-to-br from-primary-500/5 to-purple-500/5 rounded-full blur-3xl" />
        </div>

        <div className="relative max-w-4xl mx-auto text-center">
          {/* Logo */}
          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.5 }}
            className="flex justify-center mb-8"
          >
            <div className="h-20 w-20 rounded-3xl bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center shadow-2xl shadow-primary-500/30">
              <span className="text-white font-bold text-3xl">BI</span>
            </div>
          </motion.div>

          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="text-4xl lg:text-6xl font-bold text-slate-900 dark:text-white leading-tight"
          >
            {t('welcome.heroTitle')}
          </motion.h1>

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mt-6 text-lg lg:text-xl text-slate-600 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed"
          >
            {t('welcome.heroDesc')}
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.3 }}
            className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4"
          >
            <button
              onClick={handleGetStarted}
              className="flex items-center gap-3 px-8 py-4 bg-gradient-to-r from-primary-600 to-primary-700 text-white font-semibold rounded-2xl hover:from-primary-700 hover:to-primary-800 transition-all shadow-xl shadow-primary-500/25 text-lg"
            >
              {t('welcome.getStarted')}
              <ArrowRightIcon className="h-5 w-5" />
            </button>
            {!isAuthenticated && (
              <button
                onClick={handleSignIn}
                className="flex items-center gap-2 px-6 py-4 text-slate-700 dark:text-slate-300 font-medium rounded-2xl hover:bg-white dark:hover:bg-slate-800 transition-all border border-slate-200 dark:border-slate-700"
              >
                {t('welcome.haveAccount')}
              </button>
            )}
          </motion.div>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ duration: 0.5, delay: 0.5 }}
            className="mt-4 text-sm text-slate-400 dark:text-slate-500"
          >
            {t('welcome.noAccountNeeded')}
          </motion.p>
        </div>
      </div>

      {/* How it works */}
      <div className="relative px-6 py-16 lg:py-24">
        <div className="max-w-6xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-16"
          >
            <h2 className="text-3xl lg:text-4xl font-bold text-slate-900 dark:text-white">
              {t('welcome.howItWorks')}
            </h2>
            <p className="mt-4 text-slate-500 dark:text-slate-400 text-lg">
              {t('welcome.howItWorksDesc')}
            </p>
          </motion.div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
            {steps.map((step, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
                className="relative group"
              >
                <div className="p-6 bg-white dark:bg-slate-800/50 rounded-2xl border border-slate-200 dark:border-slate-700 hover:shadow-xl hover:border-primary-300 dark:hover:border-primary-700 transition-all duration-300">
                  {/* Step number */}
                  <div className="absolute -top-3 -left-3 h-8 w-8 rounded-full bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center text-white text-sm font-bold shadow-lg">
                    {index + 1}
                  </div>

                  <div className={`${step.bg} p-3 rounded-xl w-fit mb-4`}>
                    <step.icon className="h-7 w-7 text-slate-700 dark:text-slate-300" />
                  </div>

                  <h3 className="font-semibold text-lg text-slate-900 dark:text-white mb-2">
                    {step.title}
                  </h3>
                  <p className="text-sm text-slate-500 dark:text-slate-400 leading-relaxed">
                    {step.desc}
                  </p>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </div>

      {/* CTA */}
      <div className="px-6 py-16 lg:py-20">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="max-w-3xl mx-auto text-center bg-gradient-to-br from-primary-600 to-primary-800 rounded-3xl p-12 shadow-2xl shadow-primary-500/20"
        >
          <h2 className="text-3xl font-bold text-white mb-4">
            {t('welcome.ctaTitle')}
          </h2>
          <p className="text-primary-100 text-lg mb-8">
            {t('welcome.ctaDesc')}
          </p>
          <button
            onClick={handleGetStarted}
            className="flex items-center gap-3 mx-auto px-8 py-4 bg-white text-primary-700 font-semibold rounded-2xl hover:bg-primary-50 transition-all shadow-lg text-lg"
          >
            {t('welcome.startNow')}
            <ArrowRightIcon className="h-5 w-5" />
          </button>
        </motion.div>
      </div>
    </div>
  );
};

export default WelcomePage;

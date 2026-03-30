import React from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';

const NotFoundPage: React.FC = () => {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <div className="min-h-[70vh] flex flex-col items-center justify-center text-center px-4">
      <motion.div
        initial={{ scale: 0.5, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        transition={{ type: 'spring', stiffness: 200 }}
      >
        <div className="text-8xl font-bold gradient-text mb-4">404</div>
        <h1 className="text-2xl font-bold text-slate-900 dark:text-white mb-2">
          {t('errors.pageNotFound')}
        </h1>
        <p className="text-slate-500 dark:text-slate-400 mb-8 max-w-md">
          {t('errors.pageNotFoundDesc')}
        </p>
        <button
          onClick={() => navigate('/')}
          className="px-6 py-3 bg-gradient-to-r from-primary-600 to-primary-700 text-white font-medium rounded-xl hover:from-primary-700 hover:to-primary-800 transition-all shadow-lg shadow-primary-500/25"
        >
          {t('errors.goHome')}
        </button>
      </motion.div>
    </div>
  );
};

export default NotFoundPage;

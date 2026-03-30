import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import {
  ChevronDownIcon,
  ChevronUpIcon,
  CubeTransparentIcon,
  ArrowPathIcon,
  ServerStackIcon,
  ChartBarIcon,
  ShieldCheckIcon,
  GlobeAltIcon,
} from '@heroicons/react/24/outline';

const FAQItem: React.FC<{ question: string; answer: string; index: number }> = ({ question, answer, index }) => {
  const [open, setOpen] = useState(false);
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden"
    >
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between p-5 text-left bg-white dark:bg-slate-800 hover:bg-slate-50 dark:hover:bg-slate-750 transition-colors"
      >
        <span className="font-medium text-slate-900 dark:text-white pr-4">{question}</span>
        {open ? (
          <ChevronUpIcon className="h-5 w-5 text-primary-500 flex-shrink-0" />
        ) : (
          <ChevronDownIcon className="h-5 w-5 text-slate-400 flex-shrink-0" />
        )}
      </button>
      {open && (
        <motion.div
          initial={{ height: 0, opacity: 0 }}
          animate={{ height: 'auto', opacity: 1 }}
          className="px-5 pb-5 text-slate-600 dark:text-slate-300 leading-relaxed bg-white dark:bg-slate-800"
        >
          {answer}
        </motion.div>
      )}
    </motion.div>
  );
};

const AboutPage: React.FC = () => {
  const { t } = useTranslation();

  const features = [
    { icon: CubeTransparentIcon, title: t('about.feature1Title'), desc: t('about.feature1Desc') },
    { icon: ArrowPathIcon, title: t('about.feature2Title'), desc: t('about.feature2Desc') },
    { icon: ServerStackIcon, title: t('about.feature3Title'), desc: t('about.feature3Desc') },
    { icon: ChartBarIcon, title: t('about.feature4Title'), desc: t('about.feature4Desc') },
    { icon: ShieldCheckIcon, title: t('about.feature5Title'), desc: t('about.feature5Desc') },
    { icon: GlobeAltIcon, title: t('about.feature6Title'), desc: t('about.feature6Desc') },
  ];

  const faqKeys = ['faq1', 'faq2', 'faq3', 'faq4', 'faq5', 'faq6', 'faq7', 'faq8'];

  return (
    <div className="max-w-4xl mx-auto space-y-10 animate-fade-in">
      {/* Hero */}
      <div className="text-center">
        <motion.div
          initial={{ scale: 0.8, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          className="inline-flex items-center justify-center h-20 w-20 rounded-2xl bg-gradient-to-br from-primary-500 to-primary-700 shadow-lg shadow-primary-500/25 mb-6"
        >
          <span className="text-white font-bold text-3xl">BI</span>
        </motion.div>
        <h1 className="text-3xl font-bold text-slate-900 dark:text-white mb-3">
          {t('about.title')}
        </h1>
        <p className="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed">
          {t('about.description')}
        </p>
      </div>

      {/* Features grid */}
      <div>
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-6">{t('about.featuresTitle')}</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {features.map((feat, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.08 }}
              className="p-5 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 hover:shadow-lg hover:-translate-y-0.5 transition-all"
            >
              <div className="h-10 w-10 rounded-lg bg-primary-50 dark:bg-primary-900/30 flex items-center justify-center mb-3">
                <feat.icon className="h-5 w-5 text-primary-600 dark:text-primary-400" />
              </div>
              <h3 className="font-semibold text-slate-900 dark:text-white mb-1">{feat.title}</h3>
              <p className="text-sm text-slate-500 dark:text-slate-400 leading-relaxed">{feat.desc}</p>
            </motion.div>
          ))}
        </div>
      </div>

      {/* How it works */}
      <div className="bg-white dark:bg-slate-800 rounded-2xl border border-slate-200 dark:border-slate-700 p-8">
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-6">{t('about.howItWorksTitle')}</h2>
        <div className="space-y-6">
          {['step1', 'step2', 'step3', 'step4', 'step5'].map((step, i) => (
            <div key={step} className="flex gap-4">
              <div className="flex-shrink-0 h-8 w-8 rounded-full bg-primary-100 dark:bg-primary-900/40 flex items-center justify-center">
                <span className="text-sm font-bold text-primary-700 dark:text-primary-300">{i + 1}</span>
              </div>
              <div>
                <h3 className="font-semibold text-slate-900 dark:text-white">{t(`about.${step}Title`)}</h3>
                <p className="text-sm text-slate-500 dark:text-slate-400 mt-0.5">{t(`about.${step}Desc`)}</p>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* FAQ */}
      <div>
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-6">{t('about.faqTitle')}</h2>
        <div className="space-y-3">
          {faqKeys.map((key, i) => (
            <FAQItem
              key={key}
              question={t(`about.${key}Q`)}
              answer={t(`about.${key}A`)}
              index={i}
            />
          ))}
        </div>
      </div>

      {/* Tech stack */}
      <div className="bg-gradient-to-br from-primary-50 to-primary-100/50 dark:from-primary-900/20 dark:to-primary-900/10 rounded-2xl border border-primary-200 dark:border-primary-800 p-8">
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-4">{t('about.techStackTitle')}</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            { name: 'FastAPI', desc: 'Backend' },
            { name: 'React', desc: 'Frontend' },
            { name: 'PostgreSQL', desc: 'Database' },
            { name: 'LLaMA 3', desc: 'LLM' },
            { name: 'Pandas', desc: 'ETL' },
            { name: 'SQLAlchemy', desc: 'ORM' },
            { name: 'TailwindCSS', desc: 'Styling' },
            { name: 'Docker', desc: 'Deploy' },
          ].map((tech) => (
            <div key={tech.name} className="text-center p-3 bg-white/60 dark:bg-slate-800/60 rounded-xl">
              <p className="font-semibold text-slate-900 dark:text-white text-sm">{tech.name}</p>
              <p className="text-xs text-slate-500 dark:text-slate-400">{tech.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Version info */}
      <div className="text-center text-sm text-slate-400 dark:text-slate-500 pb-8">
        <p>BI Platform v2.0.0</p>
        <p className="mt-1">{t('about.copyright')}</p>
      </div>
    </div>
  );
};

export default AboutPage;

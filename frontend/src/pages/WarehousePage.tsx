import React, { useState, useRef, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useMutation } from '@tanstack/react-query';
import {
  TableCellsIcon,
  PlayIcon,
  DocumentDuplicateIcon,
  ArrowPathIcon,
  MagnifyingGlassIcon,
  SparklesIcon,
  CubeIcon,
  ChatBubbleBottomCenterTextIcon,
  PaperAirplaneIcon,
  CodeBracketIcon,
} from '@heroicons/react/24/outline';
import { toast } from 'react-hot-toast';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { warehouseApi, llmApi } from '../services/api';
import { clsx } from 'clsx';

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  sql?: string;
}

export const WarehousePage: React.FC = () => {
  const { t } = useTranslation();
  const chatEndRef = useRef<HTMLDivElement>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [sqlQuery, setSqlQuery] = useState<string>('');
  const [naturalQuery, setNaturalQuery] = useState<string>('');
  const [queryResults, setQueryResults] = useState<any>(null);
  const [activeTab, setActiveTab] = useState<'sql' | 'natural' | 'assistant'>('sql');
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState('');
  const [isChatLoading, setIsChatLoading] = useState(false);

  const { data: tables, isLoading: tablesLoading, refetch: refetchTables } = useQuery({
    queryKey: ['warehouse-tables'],
    queryFn: warehouseApi.listTables,
  });

  const { data: stats } = useQuery({
    queryKey: ['warehouse-stats'],
    queryFn: warehouseApi.getStats,
  });

  const { data: tableData } = useQuery({
    queryKey: ['table-data', selectedTable],
    queryFn: () => warehouseApi.getTableData(selectedTable!, 100, 0),
    enabled: !!selectedTable,
  });

  const executeSqlMutation = useMutation({
    mutationFn: (sql: string) => warehouseApi.query(sql),
    onSuccess: (data) => {
      setQueryResults(data);
      toast.success(`Query executed: ${data.row_count} rows returned`);
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const naturalQueryMutation = useMutation({
    mutationFn: (question: string) => llmApi.naturalQuery(question, true),
    onSuccess: (data) => {
      if (data.sql) setSqlQuery(data.sql);
      if (data.results) {
        setQueryResults({
          columns: data.results.length > 0 ? Object.keys(data.results[0]) : [],
          data: data.results,
          row_count: data.row_count || data.results.length,
        });
      }
      if (data.explanation) toast.success(data.explanation.slice(0, 100) + '...');
    },
    onError: (error: Error) => toast.error(error.message),
  });

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [chatMessages]);

  const handleExecuteQuery = () => {
    if (!sqlQuery.trim()) { toast.error('Please enter a SQL query'); return; }
    executeSqlMutation.mutate(sqlQuery);
  };

  const handleNaturalQuery = () => {
    if (!naturalQuery.trim()) { toast.error('Please enter a question'); return; }
    naturalQueryMutation.mutate(naturalQuery);
  };

  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);
    setSqlQuery(`SELECT * FROM ${tableName} LIMIT 100`);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const handleSendChat = async () => {
    if (!chatInput.trim()) return;
    const userMsg: ChatMessage = { id: Date.now().toString(), role: 'user', content: chatInput, timestamp: new Date() };
    setChatMessages(prev => [...prev, userMsg]);
    const msg = chatInput;
    setChatInput('');
    setIsChatLoading(true);

    try {
      // Try to detect if user wants SQL or Python
      const wantsSql = /sql|query|select|join|group by|where/i.test(msg);
      const wantsPython = /python|pandas|matplotlib|code|script|analyze/i.test(msg);

      let response: any;
      if (wantsSql) {
        response = await llmApi.naturalQuery(msg, false);
        const content = response.sql
          ? `Here's the SQL query:\n\n\`\`\`sql\n${response.sql}\n\`\`\`\n\n${response.explanation || ''}`
          : response.explanation || response.response || 'Could not generate SQL';
        setChatMessages(prev => [...prev, {
          id: (Date.now() + 1).toString(), role: 'assistant', content, timestamp: new Date(), sql: response.sql,
        }]);
      } else if (wantsPython) {
        response = await llmApi.explain(msg, 'Generate Python/pandas code for data analysis');
        setChatMessages(prev => [...prev, {
          id: (Date.now() + 1).toString(), role: 'assistant',
          content: response.explanation || response.response || 'No response',
          timestamp: new Date(),
        }]);
      } else {
        response = await llmApi.explain(msg, 'warehouse data exploration');
        setChatMessages(prev => [...prev, {
          id: (Date.now() + 1).toString(), role: 'assistant',
          content: response.explanation || response.response || 'No response',
          timestamp: new Date(),
        }]);
      }
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : 'Error';
      setChatMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(), role: 'assistant', content: `Error: ${errorMsg}`, timestamp: new Date(),
      }]);
    } finally {
      setIsChatLoading(false);
    }
  };

  const handleCopySql = (sql: string) => {
    setSqlQuery(sql);
    setActiveTab('sql');
    toast.success('SQL copied to editor');
  };

  const factTables = tables?.filter((t: any) => t.table_type === 'fact') || [];
  const dimensionTables = tables?.filter((t: any) => t.table_type === 'dimension') || [];

  if (tablesLoading) {
    return <div className="flex items-center justify-center h-96"><LoadingSpinner size="lg" text={t('common.loading')} /></div>;
  }

  const hasTables = tables && tables.length > 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-white">{t('warehouse.title')}</h1>
          <p className="mt-1 text-slate-500 dark:text-slate-400">{t('warehouse.subtitle')}</p>
        </div>
        <Button variant="secondary" icon={<ArrowPathIcon className="h-5 w-5" />} onClick={() => refetchTables()}>{t('common.refresh')}</Button>
      </div>

      {!hasTables ? (
        <Card variant="glass" className="py-16">
          <div className="text-center">
            <TableCellsIcon className="mx-auto h-16 w-16 text-slate-300 dark:text-slate-600" />
            <h3 className="mt-4 text-lg font-medium text-slate-900 dark:text-white">No warehouse tables found</h3>
            <p className="mt-2 text-slate-500 dark:text-slate-400">Run the ETL pipeline to populate the data warehouse</p>
          </div>
        </Card>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Left Sidebar */}
          <div className="space-y-4">
            <Card variant="glass" className="p-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-xs text-slate-500 dark:text-slate-400">Fact Tables</p>
                  <p className="text-xl font-bold text-primary-600 dark:text-primary-400">{stats?.fact_table_count || factTables.length}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500 dark:text-slate-400">Dimensions</p>
                  <p className="text-xl font-bold text-emerald-600 dark:text-emerald-400">{stats?.dimension_table_count || dimensionTables.length}</p>
                </div>
              </div>
            </Card>

            <Card variant="glass">
              <CardHeader title="Fact Tables" />
              <div className="space-y-1">
                {factTables.length === 0 ? <p className="text-sm text-slate-500 dark:text-slate-400 py-2">No fact tables</p> : factTables.map((table: any) => (
                  <button key={table.name} onClick={() => handleTableSelect(table.name)}
                    className={clsx('w-full flex items-center justify-between p-2 rounded-lg transition-all text-left',
                      selectedTable === table.name ? 'bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300' : 'hover:bg-slate-50 dark:hover:bg-slate-800'
                    )}>
                    <div className="flex items-center gap-2"><CubeIcon className="h-4 w-4 text-primary-500" /><span className="text-sm font-medium truncate">{table.display_name}</span></div>
                    <span className="text-xs text-slate-500 dark:text-slate-400">{table.row_count?.toLocaleString()}</span>
                  </button>
                ))}
              </div>
            </Card>

            <Card variant="glass">
              <CardHeader title="Dimension Tables" />
              <div className="space-y-1 max-h-64 overflow-y-auto">
                {dimensionTables.length === 0 ? <p className="text-sm text-slate-500 dark:text-slate-400 py-2">No dimension tables</p> : dimensionTables.map((table: any) => (
                  <button key={table.name} onClick={() => handleTableSelect(table.name)}
                    className={clsx('w-full flex items-center justify-between p-2 rounded-lg transition-all text-left',
                      selectedTable === table.name ? 'bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300' : 'hover:bg-slate-50 dark:hover:bg-slate-800'
                    )}>
                    <div className="flex items-center gap-2"><TableCellsIcon className="h-4 w-4 text-emerald-500" /><span className="text-sm font-medium truncate">{table.display_name}</span></div>
                    <span className="text-xs text-slate-500 dark:text-slate-400">{table.row_count?.toLocaleString()}</span>
                  </button>
                ))}
              </div>
            </Card>
          </div>

          {/* Main Content */}
          <div className="lg:col-span-3 space-y-6">
            {/* Query Interface with 3 tabs */}
            <Card variant="glass">
              <div className="flex gap-1 p-2 bg-slate-100 dark:bg-slate-800 rounded-t-xl">
                {[
                  { key: 'sql' as const, icon: CodeBracketIcon, label: 'SQL Query' },
                  { key: 'natural' as const, icon: SparklesIcon, label: t('warehouse.naturalLanguage') },
                  { key: 'assistant' as const, icon: ChatBubbleBottomCenterTextIcon, label: t('warehouse.llmAssistant') },
                ].map((tab) => (
                  <button key={tab.key} onClick={() => setActiveTab(tab.key)}
                    className={clsx('flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-medium text-sm transition-all',
                      activeTab === tab.key ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow' : 'text-slate-600 dark:text-slate-400'
                    )}>
                    <tab.icon className="h-4 w-4" />{tab.label}
                  </button>
                ))}
              </div>

              <div className="p-4">
                {activeTab === 'sql' && (
                  <div className="space-y-3">
                    <div className="relative">
                      <textarea value={sqlQuery} onChange={(e) => setSqlQuery(e.target.value)} placeholder="SELECT * FROM fact_main LIMIT 100" rows={4}
                        className="w-full px-4 py-3 bg-slate-900 text-green-400 font-mono text-sm rounded-lg border border-slate-700 focus:ring-2 focus:ring-primary-500 resize-none" />
                      <button onClick={() => copyToClipboard(sqlQuery)} className="absolute top-2 right-2 p-1.5 text-slate-400 hover:text-white transition-colors">
                        <DocumentDuplicateIcon className="h-4 w-4" />
                      </button>
                    </div>
                    <div className="flex justify-end">
                      <Button onClick={handleExecuteQuery} icon={<PlayIcon className="h-5 w-5" />} disabled={executeSqlMutation.isPending || !sqlQuery.trim()}>
                        {executeSqlMutation.isPending ? t('common.loading') : t('warehouse.runQuery')}
                      </Button>
                    </div>
                  </div>
                )}

                {activeTab === 'natural' && (
                  <div className="space-y-3">
                    <div className="relative">
                      <input type="text" value={naturalQuery} onChange={(e) => setNaturalQuery(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && handleNaturalQuery()}
                        placeholder={t('warehouse.nlPlaceholder')} className="w-full px-4 py-3 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg focus:ring-2 focus:ring-primary-500" />
                      <MagnifyingGlassIcon className="absolute right-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {['Show me total sales by month', 'What are the top 10 products?', 'Count records by category', 'Average order value by customer'].map((s) => (
                        <button key={s} onClick={() => setNaturalQuery(s)} className="px-3 py-1 text-xs bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-full hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors">{s}</button>
                      ))}
                    </div>
                    <div className="flex justify-end">
                      <Button onClick={handleNaturalQuery} icon={<SparklesIcon className="h-5 w-5" />} disabled={naturalQueryMutation.isPending || !naturalQuery.trim()}>
                        {naturalQueryMutation.isPending ? t('common.loading') : t('warehouse.generateSQL')}
                      </Button>
                    </div>
                  </div>
                )}

                {activeTab === 'assistant' && (
                  <div className="flex flex-col h-[400px]">
                    <div className="flex-1 overflow-y-auto space-y-4 mb-4">
                      {chatMessages.length === 0 ? (
                        <div className="text-center py-12 text-slate-500 dark:text-slate-400">
                          <ChatBubbleBottomCenterTextIcon className="mx-auto h-10 w-10 mb-3 text-slate-300 dark:text-slate-600" />
                          <p className="font-medium">AI Warehouse Assistant</p>
                          <p className="text-sm mt-1">Ask about your data, generate SQL or Python, or explore patterns</p>
                        </div>
                      ) : chatMessages.map((msg) => (
                        <div key={msg.id} className={clsx('flex', msg.role === 'user' ? 'justify-end' : 'justify-start')}>
                          <div className={clsx('max-w-[85%] px-4 py-3 rounded-2xl',
                            msg.role === 'user' ? 'bg-primary-500 text-white rounded-br-none' : 'bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-white rounded-bl-none'
                          )}>
                            <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                            {msg.sql && msg.role === 'assistant' && (
                              <button onClick={() => handleCopySql(msg.sql!)} className="mt-2 flex items-center gap-1 text-xs text-primary-600 dark:text-primary-400 hover:underline">
                                <PlayIcon className="h-3 w-3" /> Run this SQL
                              </button>
                            )}
                            <span className="text-xs opacity-60 mt-1 block">{msg.timestamp.toLocaleTimeString()}</span>
                          </div>
                        </div>
                      ))}
                      {isChatLoading && (
                        <div className="flex justify-start">
                          <div className="bg-slate-100 dark:bg-slate-800 px-4 py-3 rounded-2xl rounded-bl-none">
                            <div className="flex items-center gap-2"><ArrowPathIcon className="h-4 w-4 animate-spin text-primary-500" /><span className="text-sm text-slate-500">Thinking...</span></div>
                          </div>
                        </div>
                      )}
                      <div ref={chatEndRef} />
                    </div>
                    <div className="border-t border-slate-200 dark:border-slate-700 pt-3">
                      <div className="flex gap-2">
                        <input type="text" value={chatInput} onChange={(e) => setChatInput(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && handleSendChat()}
                          placeholder={t('warehouse.askAboutData')} disabled={isChatLoading}
                          className="flex-1 px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl focus:ring-2 focus:ring-primary-500 transition-all" />
                        <Button onClick={handleSendChat} disabled={!chatInput.trim() || isChatLoading} icon={<PaperAirplaneIcon className="h-5 w-5" />} />
                      </div>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {['Write SQL for top customers', 'Generate Python analysis', 'Explain the schema', 'Show table relationships'].map((p) => (
                          <button key={p} onClick={() => { setChatInput(p); }} disabled={isChatLoading}
                            className="px-3 py-1 text-xs bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-full hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors disabled:opacity-50">{p}</button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </Card>

            {/* Results */}
            {activeTab !== 'assistant' && (
              <Card variant="glass">
                <CardHeader title={t('warehouse.results')} subtitle={queryResults ? `${queryResults.row_count} ${t('warehouse.rowsReturned')}` : undefined} />
                <div className="overflow-x-auto">
                  {queryResults ? (
                    <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                      <thead className="bg-slate-50 dark:bg-slate-800/50">
                        <tr>{queryResults.columns?.map((col: string) => (<th key={col} className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">{col}</th>))}</tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                        {queryResults.data?.slice(0, 50).map((row: any, i: number) => (
                          <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                            {queryResults.columns?.map((col: string) => (<td key={col} className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap">{row[col]?.toString() ?? 'NULL'}</td>))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : tableData ? (
                    <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                      <thead className="bg-slate-50 dark:bg-slate-800/50">
                        <tr>{tableData.data?.[0] && Object.keys(tableData.data[0]).map((col: string) => (<th key={col} className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">{col}</th>))}</tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                        {tableData.data?.slice(0, 50).map((row: any, i: number) => (
                          <tr key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                            {Object.values(row).map((value: any, ci: number) => (<td key={ci} className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap">{value?.toString() ?? 'NULL'}</td>))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div className="text-center py-12">
                      <TableCellsIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                      <p className="mt-4 text-slate-500 dark:text-slate-400">Select a table or run a query to see results</p>
                    </div>
                  )}
                </div>
                {(queryResults?.row_count > 50 || tableData?.row_count > 50) && (
                  <div className="p-4 border-t border-slate-200 dark:border-slate-700 text-center">
                    <p className="text-sm text-slate-500 dark:text-slate-400">Showing first 50 rows of {queryResults?.row_count || tableData?.row_count}</p>
                  </div>
                )}
              </Card>
            )}

            {/* Table Schema */}
            {selectedTable && tableData && activeTab !== 'assistant' && (
              <Card variant="glass">
                <CardHeader title={`${selectedTable} Schema`} />
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                    <thead className="bg-slate-50 dark:bg-slate-800/50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">Column</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">Type</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                      {tables?.find((t: any) => t.name === selectedTable)?.columns?.map((col: any) => (
                        <tr key={col.name} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                          <td className="px-4 py-2 text-sm font-mono text-slate-900 dark:text-white">{col.name}</td>
                          <td className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">{col.type}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default WarehousePage;

"use client"

import { useState, useEffect, useRef } from "react"
import { 
  Database, Grid3X3, Play, HelpCircle, RotateCcw, ChevronDown, Search, Settings, User, 
  AlertCircle, CheckCircle, Loader2, Plus, Upload, FileText, MapPin, Hash, TreePine,
  Building, X, Download, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight
} from "lucide-react"

import { DatabaseAPI, TableInfo, QueryResult, TableScanResult } from "../lib/api"

interface QueryState {
  isLoading: boolean;
  result: QueryResult | TableScanResult | null;
  error: string | null;
  executionTime: number;
}

interface CreateTableModal {
  isOpen: boolean;
  tableName: string;
  csvFile: string;
  indexType: 'sequential' | 'isam' | 'hash' | 'btree' | 'rtree';
  indexField: string;
}

// TableInfo ya está importado desde ../lib/api

export default function DatabaseManager() {
  const [selectedTable, setSelectedTable] = useState<string>("")
  const [sqlQuery, setSqlQuery] = useState("select * from students where math_score > 80;")
  const [tables, setTables] = useState<TableInfo[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [isConnected, setIsConnected] = useState(false)
  const [connectionStatus, setConnectionStatus] = useState<'checking' | 'connected' | 'disconnected'>('checking')
  const fileInputRef = useRef<HTMLInputElement>(null)
  
  // Estados de paginación
  const [currentPage, setCurrentPage] = useState(1)
  const [recordsPerPage] = useState(100)
  const [totalRecords, setTotalRecords] = useState(0)
  
  const [queryState, setQueryState] = useState<QueryState>({
    isLoading: false,
    result: null,
    error: null,
    executionTime: 0
  })

  const [createModal, setCreateModal] = useState<CreateTableModal>({
    isOpen: false,
    tableName: '',
    csvFile: '',
    indexType: 'btree',
    indexField: '0'
  })

  const [notifications, setNotifications] = useState<Array<{
    id: string;
    type: 'success' | 'error' | 'info';
    message: string;
  }>>([])

  // ========== LIFECYCLE ==========
  
  useEffect(() => {
    checkConnection()
    const interval = setInterval(checkConnection, 30000)
    return () => clearInterval(interval)
  }, [])

  // Reset página cuando cambia el resultado
  useEffect(() => {
    console.log('🎯 UseEffect - queryState.result cambió:', queryState.result);
    if (queryState.result) {
      setCurrentPage(1)
      const rows = 'rows' in queryState.result ? queryState.result.rows || [] : []
      console.log('📊 Filas extraídas:', rows);
      console.log('📊 Cantidad de filas:', rows.length);
      setTotalRecords(rows.length)
    }
  }, [queryState.result])

  // ========== UTILITIES ==========

  const addNotification = (type: 'success' | 'error' | 'info', message: string) => {
    const id = Date.now().toString()
    setNotifications(prev => [...prev, { id, type, message }])
    setTimeout(() => {
      setNotifications(prev => prev.filter(n => n.id !== id))
    }, 5000)
  }

  const removeNotification = (id: string) => {
    setNotifications(prev => prev.filter(n => n.id !== id))
  }

  // ========== PAGINATION HELPERS ==========

  const totalPages = Math.ceil(totalRecords / recordsPerPage)
  const startRecord = (currentPage - 1) * recordsPerPage + 1
  const endRecord = Math.min(currentPage * recordsPerPage, totalRecords)

  const goToPage = (page: number) => {
    setCurrentPage(Math.max(1, Math.min(page, totalPages)))
  }

  const goToFirstPage = () => goToPage(1)
  const goToLastPage = () => goToPage(totalPages)
  const goToPreviousPage = () => goToPage(currentPage - 1)
  const goToNextPage = () => goToPage(currentPage + 1)

  // ========== API CALLS ==========

  const checkConnection = async () => {
    setConnectionStatus('checking')
    const response = await DatabaseAPI.getHealth()
    const connected = response.success
    setIsConnected(connected)
    setConnectionStatus(connected ? 'connected' : 'disconnected')
    
    if (!connected) {
      addNotification('error', 'No se pudo conectar con el servidor. Asegúrate de que la API esté corriendo en localhost:8000')
    }
  }

  const loadTables = async () => {
    const response = await DatabaseAPI.getTables()
    if (response.success && response.data) {
      setTables(response.data)
      if (response.data.length > 0 && !selectedTable) {
        setSelectedTable(response.data[0].table_name)
      }
      addNotification('success', `Cargadas ${response.data.length} tablas`)
    } else {
      addNotification('error', response.message || 'Error al cargar tablas')
    }
  }

  const executeQuery = async () => {
    if (!sqlQuery.trim()) {
      addNotification('error', 'Por favor ingresa una consulta SQL')
      return
    }

    if (!DatabaseAPI.isValidSQLQuery(sqlQuery)) {
      addNotification('error', 'Consulta SQL no válida. Debe comenzar con SELECT, INSERT, DELETE o CREATE')
      return
    }

    setQueryState({ isLoading: true, result: null, error: null, executionTime: 0 })
    
    const startTime = performance.now()
    console.log('🔍 Ejecutando consulta:', sqlQuery);
    
    const response = await DatabaseAPI.executeSQL(sqlQuery)
    console.log('📥 Respuesta completa de API:', response);
    
    const endTime = performance.now()
    
    if (response.success && response.data) {
      console.log('✅ Datos recibidos:', response.data);
      console.log('📋 Columnas:', response.data.columns);
      console.log('📊 Filas:', response.data.rows);
      console.log('📊 Tipo de filas:', typeof response.data.rows);
      console.log('📊 Es array?:', Array.isArray(response.data.rows));
      
      setQueryState({
        isLoading: false,
        result: response.data,
        error: null,
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('success', `Consulta ejecutada exitosamente en ${DatabaseAPI.formatExecutionTime(response.data.execution_time)}`)
      
      if (sqlQuery.toLowerCase().trim().startsWith('create')) {
        loadTables()
      }
    } else {
      console.log('❌ Error en respuesta:', response);
      setQueryState({
        isLoading: false,
        result: null,
        error: response.message || "Error desconocido",
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('error', response.message || 'Error al ejecutar consulta')
    }
  }

  const scanSelectedTable = async () => {
    if (!selectedTable) {
      addNotification('error', 'Selecciona una tabla primero')
      return
    }

    setQueryState({ isLoading: true, result: null, error: null, executionTime: 0 })
    
    const startTime = performance.now()
    const response = await DatabaseAPI.scanTable(selectedTable, 100)
    const endTime = performance.now()
    
    if (response.success && response.data) {
      setQueryState({
        isLoading: false,
        result: response.data,
        error: null,
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('success', `Tabla escaneada: ${response.data.total_records} registros`)
    } else {
      setQueryState({
        isLoading: false,
        result: null,
        error: response.message || "Error al escanear tabla",
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('error', response.message || 'Error al escanear tabla')
    }
  }

  const createTable = async () => {
    if (!createModal.tableName || !createModal.csvFile || !createModal.indexField) {
      addNotification('error', 'Completa todos los campos')
      return
    }

    const response = await DatabaseAPI.createTable(
      createModal.tableName,
      createModal.csvFile,
      createModal.indexType,
      createModal.indexField
    )

    if (response.success) {
      addNotification('success', `Tabla "${createModal.tableName}" creada exitosamente`)
      
      const newTable: TableInfo = {
        table_name: createModal.tableName,
        index_type: createModal.indexType,
        record_count: 0,
        field_index: parseInt(createModal.indexField) || 0
      }
      
      setTables(prevTables => [...prevTables, newTable])
      setSelectedTable(createModal.tableName)
      
      setCreateModal({ isOpen: false, tableName: '', csvFile: '', indexType: 'btree', indexField: '0' })
    } else {
      addNotification('error', response.message || 'Error al crear tabla')
    }
  }

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    if (!file.name.endsWith('.csv')) {
      addNotification('error', 'Solo se permiten archivos CSV')
      return
    }

    const response = await DatabaseAPI.uploadCSV(file)
    if (response.success) {
      addNotification('success', `Archivo "${file.name}" subido exitosamente`)
      loadTables()
    } else {
      addNotification('error', response.message || 'Error al subir archivo')
    }
  }

  const clearQuery = () => {
    setSqlQuery("")
    setQueryState({ isLoading: false, result: null, error: null, executionTime: 0 })
    setCurrentPage(1)
    setTotalRecords(0)
  }

  // ========== RENDER HELPERS ==========

  const getIndexIcon = (indexType: string) => {
    const icons: Record<string, React.ComponentType<{ className?: string }>> = {
      'sequential': FileText,
      'isam': Building,
      'hash': Hash,
      'btree': TreePine,
      'rtree': MapPin
    }
    const IconComponent = icons[indexType] || Grid3X3
    return <IconComponent className="w-4 h-4" />
  }

  const getConnectionStatusColor = () => {
    switch (connectionStatus) {
      case 'connected': return 'bg-green-500'
      case 'disconnected': return 'bg-red-500'
      case 'checking': return 'bg-yellow-500 animate-pulse'
      default: return 'bg-gray-500'
    }
  }

  const getConnectionStatusText = () => {
    switch (connectionStatus) {
      case 'connected': return 'Conectado'
      case 'disconnected': return 'Desconectado'
      case 'checking': return 'Verificando...'
      default: return 'Desconocido'
    }
  }

  const renderPaginationControls = () => {
    if (totalPages <= 1) return null

    return (
      <div className="flex items-center justify-between px-6 py-4 bg-slate-50/80 border-t border-slate-200">
        <div className="flex items-center gap-2 text-sm text-slate-600">
          <span>Mostrando {startRecord} a {endRecord} de {totalRecords.toLocaleString()} registros</span>
        </div>
        
        <div className="flex items-center gap-2">
          <button
            onClick={goToFirstPage}
            disabled={currentPage === 1}
            className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Primera página"
          >
            <ChevronsLeft className="w-4 h-4" />
          </button>
          
          <button
            onClick={goToPreviousPage}
            disabled={currentPage === 1}
            className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Página anterior"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
          
          <div className="flex items-center gap-1">
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              let pageNum: number
              if (totalPages <= 5) {
                pageNum = i + 1
              } else if (currentPage <= 3) {
                pageNum = i + 1
              } else if (currentPage >= totalPages - 2) {
                pageNum = totalPages - 4 + i
              } else {
                pageNum = currentPage - 2 + i
              }
              
              return (
                <button
                  key={pageNum}
                  onClick={() => goToPage(pageNum)}
                  className={`w-8 h-8 rounded-lg text-sm font-medium transition-colors ${
                    currentPage === pageNum
                      ? 'bg-blue-500 text-white'
                      : 'border border-slate-200 hover:bg-slate-100'
                  }`}
                >
                  {pageNum}
                </button>
              )
            })}
          </div>
          
          <button
            onClick={goToNextPage}
            disabled={currentPage === totalPages}
            className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Página siguiente"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
          
          <button
            onClick={goToLastPage}
            disabled={currentPage === totalPages}
            className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Última página"
          >
            <ChevronsRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    )
  }

  const renderResultTable = () => {
    console.log('🎨 Renderizando tabla - queryState.result:', queryState.result);
    
    if (!queryState.result) {
      console.log('❌ No hay queryState.result');
      return null;
    }

    const columns = queryState.result.columns || []
    const allRows: Array<any[]> = 'rows' in queryState.result ? queryState.result.rows || [] : []

    console.log('📋 Columnas extraídas:', columns);
    console.log('📊 Filas extraídas:', allRows);
    console.log('📊 Cantidad de filas:', allRows.length);
    console.log('📊 Primera fila:', allRows[0]);

    if (allRows.length === 0) {
      console.log('❌ allRows.length === 0, mostrando "No se encontraron resultados"');
      return (
        <div className="flex items-center justify-center h-full">
          <div className="text-center text-slate-500">
            <AlertCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p className="text-lg">No se encontraron resultados</p>
            <p className="text-sm mt-2 text-red-500">Debug: allRows.length = {allRows.length}</p>
          </div>
        </div>
      )
    }

    // Aplicar paginación
    const startIndex = (currentPage - 1) * recordsPerPage
    const endIndex = startIndex + recordsPerPage
    const paginatedRows = allRows.slice(startIndex, endIndex)

    console.log('📄 Filas paginadas:', paginatedRows);
    console.log('📄 Cantidad paginada:', paginatedRows.length);

    return (
      <div className="flex flex-col h-full">
        {/* Tabla con scroll limitado */}
        <div className="flex-1 overflow-auto">
          <table className="w-full">
            <thead className="bg-slate-50/80 sticky top-0 z-10">
              <tr>
                {columns.map((column, index) => (
                  <th key={index} className="px-6 py-4 text-left text-xs font-semibold text-slate-600 uppercase tracking-wider">
                    <div className="flex items-center gap-2">
                      {column} <ChevronDown className="w-3 h-3 opacity-60" />
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white/60 backdrop-blur-sm divide-y divide-slate-200">
              {paginatedRows.map((row: any[], rowIndex) => (
                <tr key={startIndex + rowIndex} className="hover:bg-blue-50/50 transition-colors duration-150">
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex} className="px-6 py-4 text-sm text-slate-700">
                      {typeof cell === 'number' && !isNaN(cell) ? (
                        <span className="font-mono">{cell}</span>
                      ) : (
                        String(cell || '')
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        {/* Controles de paginación */}
        {renderPaginationControls()}
      </div>
    )
  }

  const filteredTables = tables.filter(table => 
    table.table_name.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex">
      {/* Notifications */}
      <div className="fixed top-4 right-4 z-50 space-y-2">
        {notifications.map((notification) => (
          <div
            key={notification.id}
            className={`flex items-center gap-3 px-4 py-3 rounded-lg shadow-lg backdrop-blur-sm transition-all duration-300 ${
              notification.type === 'success' ? 'bg-green-500/90 text-white' :
              notification.type === 'error' ? 'bg-red-500/90 text-white' :
              'bg-blue-500/90 text-white'
            }`}
          >
            {notification.type === 'success' && <CheckCircle className="w-5 h-5" />}
            {notification.type === 'error' && <AlertCircle className="w-5 h-5" />}
            <span className="text-sm font-medium">{notification.message}</span>
            <button 
              onClick={() => removeNotification(notification.id)}
              className="p-1 hover:bg-white/20 rounded"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        ))}
      </div>

      {/* Sidebar */}
      <div className="w-72 bg-white/80 backdrop-blur-sm shadow-xl border-r border-slate-200/60">
        {/* Header */}
        <div className="p-6 border-b border-slate-200/60 bg-gradient-to-r from-blue-600 to-indigo-600">
          <div className="flex items-center gap-3 text-white">
            <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
              <Database className="w-6 h-6" />
            </div>
            <div>
              <h1 className="font-bold text-lg">BDII Manager</h1>
              <div className="flex items-center gap-2 text-sm">
                <span className="text-blue-100">Proyecto 1</span>
                <div className={`w-2 h-2 rounded-full ${getConnectionStatusColor()}`} />
                <span className="text-xs text-blue-100">
                  {getConnectionStatusText()}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Search & Actions */}
        <div className="p-4 space-y-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              placeholder="Buscar tablas..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-lg bg-slate-50/50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          
          <div className="flex gap-2">
            <button
              onClick={() => setCreateModal({ ...createModal, isOpen: true })}
              className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors text-sm"
            >
              <Plus className="w-4 h-4" />
              Nueva Tabla
            </button>
            
            <button
              onClick={() => fileInputRef.current?.click()}
              className="flex items-center justify-center gap-2 px-3 py-2 border border-slate-200 text-slate-700 rounded-lg hover:bg-slate-50 transition-colors"
              title="Subir CSV"
            >
              <Upload className="w-4 h-4" />
            </button>
            
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileUpload}
              className="hidden"
            />
          </div>

          {/* Controles adicionales */}
          {tables.length === 0 ? (
            <div className="p-3 bg-slate-50 rounded-lg border border-slate-200">
              <p className="text-xs text-slate-600 mb-2">Panel limpio - Solo verás las tablas que crees</p>
              <button
                onClick={loadTables}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 text-xs bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 transition-colors"
              >
                <Search className="w-3 h-3" />
                Cargar Tablas Existentes del Servidor
              </button>
            </div>
          ) : (
            <div className="flex gap-2">
              <button
                onClick={loadTables}
                className="flex-1 flex items-center justify-center gap-1 px-2 py-1 text-xs bg-blue-50 text-blue-700 rounded-lg hover:bg-blue-100 transition-colors"
                title="Sincronizar con servidor"
              >
                <RotateCcw className="w-3 h-3" />
                Sync
              </button>
              <button
                onClick={() => {
                  setTables([])
                  setSelectedTable("")
                  addNotification('info', 'Vista limpiada')
                }}
                className="flex-1 flex items-center justify-center gap-1 px-2 py-1 text-xs bg-red-50 text-red-700 rounded-lg hover:bg-red-100 transition-colors"
                title="Limpiar vista (no afecta el servidor)"
              >
                <X className="w-3 h-3" />
                Limpiar
              </button>
            </div>
          )}
        </div>

        {/* Tables */}
        <div className="px-4 pb-4">
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-slate-600 uppercase tracking-wider">
              Tablas ({filteredTables.length})
            </h3>
            <button
              onClick={loadTables}
              className="p-1 hover:bg-slate-200 rounded-lg transition-colors"
              title="Recargar tablas"
            >
              <RotateCcw className="w-3 h-3 text-slate-500" />
            </button>
          </div>
          
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {filteredTables.map((table) => (
              <button
                key={table.table_name}
                onClick={() => setSelectedTable(table.table_name)}
                className={`w-full flex items-center gap-3 px-4 py-3 text-sm rounded-xl transition-all duration-200 group ${
                  selectedTable === table.table_name
                    ? "bg-gradient-to-r from-blue-500 to-indigo-500 text-white shadow-lg shadow-blue-500/25"
                    : "text-slate-700 hover:bg-slate-100 hover:shadow-md"
                }`}
              >
                <div className={`p-2 rounded-lg transition-colors ${
                  selectedTable === table.table_name 
                    ? "bg-white/20" 
                    : "bg-slate-200 group-hover:bg-slate-300"
                }`}>
                  {getIndexIcon(table.index_type)}
                </div>
                <div className="flex-1 text-left">
                  <div className="font-medium">{table.table_name}</div>
                  <div className={`text-xs ${
                    selectedTable === table.table_name ? "text-blue-100" : "text-slate-500"
                  }`}>
                    {table.record_count?.toLocaleString() || 0} registros • {DatabaseAPI.getIndexTypeLabel(table.index_type)}
                  </div>
                </div>
              </button>
            ))}
          </div>
          
          {filteredTables.length === 0 && (
            <div className="text-center py-8 text-slate-500">
              <Database className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p className="text-sm">No hay tablas disponibles</p>
              <button
                onClick={() => setCreateModal({ ...createModal, isOpen: true })}
                className="mt-2 text-blue-500 hover:text-blue-600 text-sm underline"
              >
                Crear primera tabla
              </button>
            </div>
          )}
        </div>

        {/* User Profile */}
        <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-slate-200/60 bg-white/50 backdrop-blur-sm">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-indigo-500 rounded-full flex items-center justify-center">
              <User className="w-4 h-4 text-white" />
            </div>
            <div className="flex-1">
              <div className="text-sm font-medium text-slate-700">Estudiante UTEC</div>
              <div className="text-xs text-slate-500">BDII - Proyecto 1</div>
            </div>
            <button 
              onClick={checkConnection}
              className="p-1 hover:bg-slate-200 rounded-lg transition-colors"
              title="Verificar conexión"
            >
              <Settings className="w-4 h-4 text-slate-500" />
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* SQL Editor - Fixed height */}
        <div className="bg-white/80 backdrop-blur-sm shadow-sm border-b border-slate-200/60 p-6 flex-shrink-0">
          <div className="mb-4">
            <h2 className="text-xl font-semibold text-slate-800 mb-2">Editor SQL</h2>
            <p className="text-slate-600 text-sm">
              Ejecuta consultas SQL personalizadas o explora la tabla{" "}
              {selectedTable && (
                <span className="font-medium text-blue-600">{selectedTable}</span>
              )}
            </p>
          </div>
          
          <div className="relative">
            <textarea
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
              className="w-full h-32 p-4 border border-slate-200 rounded-xl font-mono text-sm bg-slate-50/50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none shadow-sm"
              placeholder="Escribe tu consulta SQL aquí...&#10;Ejemplos:&#10;• select * from students where math_score > 80&#10;• create table test from file 'datos/data.csv' using index btree('0')&#10;• insert into students values ('999', 'Juan', 'male', 'group A', '85', '90', '88')"
            />
            <div className="absolute bottom-3 right-3 flex gap-1">
              <span className="px-2 py-1 bg-slate-200 text-slate-600 text-xs rounded-md font-mono">SQL</span>
            </div>
          </div>

          {/* Buttons */}
          <div className="flex gap-3 mt-6">
            <button 
              onClick={executeQuery}
              disabled={queryState.isLoading || !isConnected}
              className="flex items-center gap-2 bg-gradient-to-r from-blue-500 to-indigo-500 text-white px-6 py-3 rounded-xl hover:from-blue-600 hover:to-indigo-600 transition-all duration-200 shadow-lg shadow-blue-500/25 hover:shadow-xl hover:shadow-blue-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {queryState.isLoading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Play className="w-4 h-4" />
              )}
              <span className="font-medium">
                {queryState.isLoading ? 'Ejecutando...' : 'Ejecutar'}
              </span>
            </button>
            
            <button
              onClick={scanSelectedTable}
              disabled={!selectedTable || queryState.isLoading || !isConnected}
              className="flex items-center gap-2 border border-blue-200 text-blue-700 px-6 py-3 rounded-xl hover:bg-blue-50 hover:border-blue-300 transition-all duration-200 bg-white/60 backdrop-blur-sm disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Grid3X3 className="w-4 h-4" />
              <span className="font-medium">Explorar Tabla</span>
            </button>
            
            <button 
              onClick={clearQuery}
              className="flex items-center gap-2 border border-slate-200 text-slate-700 px-6 py-3 rounded-xl hover:bg-slate-50 hover:border-slate-300 transition-all duration-200 bg-white/60 backdrop-blur-sm"
            >
              <RotateCcw className="w-4 h-4" />
              <span className="font-medium">Limpiar</span>
            </button>
          </div>
        </div>

        {/* Results - Flexible height with scroll */}
        <div className="flex-1 p-6 overflow-hidden">
          {/* Status Bar */}
          <div className="mb-4 bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200/60 px-6 py-3 rounded-xl backdrop-blur-sm">
            <div className="flex justify-between items-center text-sm">
              <div className="flex items-center gap-4">
                <span className="text-blue-700 font-medium flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ${getConnectionStatusColor()}`}></div>
                  {isConnected ? `API: ${getConnectionStatusText()}` : 'API: Desconectado'}
                </span>
                {selectedTable && (
                  <span className="text-blue-600">Tabla: {selectedTable}</span>
                )}
                {queryState.result && totalRecords > 0 && (
                  <span className="text-blue-600">
                    Página {currentPage} de {totalPages} ({totalRecords.toLocaleString()} registros totales)
                  </span>
                )}
              </div>
              <div className="flex items-center gap-4 text-blue-600">
                {queryState.executionTime > 0 && (
                  <>
                    <span>Tiempo: {DatabaseAPI.formatExecutionTime(queryState.executionTime)}</span>
                    <span>•</span>
                  </>
                )}
                <span className={`font-medium ${isConnected ? 'text-green-600' : 'text-red-600'}`}>
                  {isConnected ? 'Ready' : 'Offline'}
                </span>
              </div>
            </div>
          </div>

          {/* Results Table Container with fixed height and scroll */}
          <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-xl border border-slate-200/60 overflow-hidden flex flex-col" style={{ height: 'calc(100vh - 400px)' }}>
            <div className="p-6 border-b border-slate-200/60 bg-gradient-to-r from-slate-50 to-slate-100 flex-shrink-0">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-semibold text-slate-800">Resultados</h3>
                  <p className="text-slate-600 text-sm">
                    {selectedTable ? `Tabla: ${selectedTable}` : 'Selecciona una tabla'}
                  </p>
                </div>
                <div className="flex items-center gap-4 text-sm text-slate-600">
                  {queryState.error ? (
                    <div className="flex items-center gap-2 text-red-600">
                      <AlertCircle className="w-4 h-4" />
                      Error en consulta
                    </div>
                  ) : queryState.result ? (
                    <div className="flex items-center gap-2 text-green-600">
                      <CheckCircle className="w-4 h-4" />
                      {totalRecords.toLocaleString()} registros totales
                    </div>
                  ) : (
                    <div className="flex items-center gap-2">
                      <div className="w-2 h-2 bg-slate-400 rounded-full"></div>
                      Listo para consultas
                    </div>
                  )}
                </div>
              </div>
            </div>
            
            {/* Table content with controlled scroll */}
            <div className="flex-1 overflow-hidden">
              {queryState.isLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Loader2 className="w-8 h-8 mx-auto mb-4 animate-spin text-blue-500" />
                    <p className="text-slate-600">Ejecutando consulta...</p>
                  </div>
                </div>
              ) : queryState.error ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center max-w-md">
                    <AlertCircle className="w-12 h-12 mx-auto mb-4 text-red-500" />
                    <h4 className="text-lg font-semibold text-red-700 mb-2">Error en la Consulta</h4>
                    <p className="text-red-600 text-sm bg-red-50 p-4 rounded-lg">
                      {queryState.error}
                    </p>
                  </div>
                </div>
              ) : queryState.result ? (
                renderResultTable()
              ) : (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Database className="w-16 h-16 mx-auto mb-4 text-slate-300" />
                    <h4 className="text-lg font-semibold text-slate-600 mb-2">¡Bienvenido al BDII Manager!</h4>
                    <p className="text-slate-500 text-sm mb-4">
                      Ejecuta una consulta SQL o explora una tabla para ver los resultados aquí
                    </p>
                    <div className="space-y-2 text-xs text-slate-400">
                      <p>💡 Tip: Usa "select * from {selectedTable || 'tabla'}" para ver todos los registros</p>
                      <p>🚀 Soporta 5 tipos de índices: Sequential, ISAM, Hash, B+Tree, R-Tree</p>
                      <p>📊 Los resultados se paginarán automáticamente en grupos de 100 registros</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Create Table Modal */}
      {createModal.isOpen && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-2xl shadow-2xl max-w-md w-full">
            <div className="p-6 border-b border-slate-200">
              <h3 className="text-lg font-semibold text-slate-800">Crear Nueva Tabla</h3>
              <p className="text-slate-600 text-sm mt-1">
                Configura los parámetros para la nueva tabla
              </p>
            </div>
            
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Nombre de la tabla
                </label>
                <input
                  type="text"
                  value={createModal.tableName}
                  onChange={(e) => setCreateModal({...createModal, tableName: e.target.value})}
                  className="w-full px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="ej: estudiantes"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Archivo CSV
                </label>
                <input
                  type="text"
                  value={createModal.csvFile}
                  onChange={(e) => setCreateModal({...createModal, csvFile: e.target.value})}
                  className="w-full px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="datos/StudentsPerformance.csv"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Tipo de índice
                </label>
                <select
                  value={createModal.indexType}
                  onChange={(e) => setCreateModal({...createModal, indexType: e.target.value as any})}
                  className="w-full px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="sequential">📝 Sequential File</option>
                  <option value="isam">🏗️ ISAM</option>
                  <option value="hash">#️⃣ Extendible Hash</option>
                  <option value="btree">🌳 B+ Tree</option>
                  <option value="rtree">🗺️ R-Tree</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Campo del índice
                </label>
                <input
                  type="text"
                  value={createModal.indexField}
                  onChange={(e) => setCreateModal({...createModal, indexField: e.target.value})}
                  className="w-full px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="0 (número de columna)"
                />
              </div>
            </div>
            
            <div className="p-6 border-t border-slate-200 flex gap-3">
              <button
                onClick={() => setCreateModal({...createModal, isOpen: false})}
                className="flex-1 px-4 py-2 border border-slate-200 text-slate-700 rounded-lg hover:bg-slate-50"
              >
                Cancelar
              </button>
              <button
                onClick={createTable}
                className="flex-1 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600"
              >
                Crear Tabla
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
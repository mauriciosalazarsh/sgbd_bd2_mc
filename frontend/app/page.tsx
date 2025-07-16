"use client"

import { useState, useEffect, useRef } from "react"
import { 
  Database, Grid3X3, Play, HelpCircle, RotateCcw, ChevronDown, Search, Settings, User, 
  AlertCircle, CheckCircle, Loader2, Plus, Upload, FileText, MapPin, Hash, TreePine,
  Building, X, Download, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight,
  BookOpen, MessageSquare, Type, Zap, Image, Music, Camera, Headphones
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

interface CreateTextIndexModal {
  isOpen: boolean;
  tableName: string;
  csvFile: string;
  textFields: string[];
  language: 'spanish' | 'english';
}

interface TextSearchModal {
  isOpen: boolean;
  tableName: string;
  query: string;
  k: number;
  fields: string[];
}

interface MultimediaSearchModal {
  isOpen: boolean;
  tableName: string;
  queryFile: File | null;
  queryFilePath: string;
  k: number;
  fields: string[];
  method: 'SIFT' | 'ResNet50' | 'InceptionV3' | 'MFCC' | 'Spectrogram' | 'Comprehensive';
  mediaType: 'image' | 'audio';
}

export default function DatabaseManager() {
  const [selectedTable, setSelectedTable] = useState<string>("")
  const [sqlQuery, setSqlQuery] = useState("")
  const [tables, setTables] = useState<TableInfo[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [isConnected, setIsConnected] = useState(false)
  const [connectionStatus, setConnectionStatus] = useState<'checking' | 'connected' | 'disconnected'>('checking')
  const fileInputRef = useRef<HTMLInputElement>(null)
  const pickleInputRef = useRef<HTMLInputElement>(null)
  
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

  // NUEVO: Estados para tablas textuales
  const [createTextModal, setCreateTextModal] = useState<CreateTextIndexModal>({
    isOpen: false,
    tableName: '',
    csvFile: '',
    textFields: [],
    language: 'spanish'
  })

  const [textSearchModal, setTextSearchModal] = useState<TextSearchModal>({
    isOpen: false,
    tableName: '',
    query: '',
    k: 10,
    fields: ['*']
  })

  const [multimediaSearchModal, setMultimediaSearchModal] = useState<MultimediaSearchModal>({
    isOpen: false,
    tableName: '',
    queryFile: null,
    queryFilePath: '',
    k: 10,
    fields: ['*'],
    method: 'SIFT',
    mediaType: 'image'
  })

  // Estados para vista expandida en línea
  const [expandedMedia, setExpandedMedia] = useState<{
    type: 'image' | 'audio' | null;
    path: string;
    title: string;
    index: number;
  }>({
    type: null,
    path: '',
    title: '',
    index: -1
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

  useEffect(() => {
    if (queryState.result) {
      setCurrentPage(1)
      const rows = 'rows' in queryState.result ? queryState.result.rows || [] : []
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
    
    const response = await DatabaseAPI.executeSQL(sqlQuery)
    
    const endTime = performance.now()
    
    if (response.success && response.data) {
      setQueryState({
        isLoading: false,
        result: {
          ...response.data,
          query_type: 'text_search',
          execution_time: (endTime - startTime) / 1000,
        },
        error: null,
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('success', `Consulta ejecutada exitosamente en ${DatabaseAPI.formatExecutionTime(response.data.execution_time)}`)
      
      if (sqlQuery.toLowerCase().trim().startsWith('create')) {
        loadTables()
      }
    } else {
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
        result: {
          ...response.data,
          query_type: 'text_search',
          execution_time: (endTime - startTime) / 1000,
        },
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

  // NUEVO: Función para crear índice textual
  const createTextIndex = async () => {
    if (!createTextModal.tableName || !createTextModal.csvFile || createTextModal.textFields.length === 0) {
      addNotification('error', 'Completa todos los campos y agrega al menos un campo de texto')
      return
    }

    const response = await DatabaseAPI.createTextIndex(
      createTextModal.tableName,
      createTextModal.csvFile,
      createTextModal.textFields,
      createTextModal.language
    )

    if (response.success) {
      addNotification('success', `Índice textual "${createTextModal.tableName}" creado exitosamente`)
      
      const newTable: TableInfo = {
        table_name: createTextModal.tableName,
        index_type: 'SPIMI',
        record_count: 0,
        field_index: 0
      }
      
      setTables(prevTables => [...prevTables, newTable])
      setSelectedTable(createTextModal.tableName)
      
      setCreateTextModal({ 
        isOpen: false, 
        tableName: '', 
        csvFile: '', 
        textFields: [], 
        language: 'spanish' 
      })
    } else {
      addNotification('error', response.message || 'Error al crear índice textual')
    }
  }

  // NUEVO: Función para búsqueda textual
  const executeTextSearch = async () => {
    if (!textSearchModal.tableName || !textSearchModal.query.trim()) {
      addNotification('error', 'Selecciona una tabla y escribe una consulta')
      return
    }

    setQueryState({ isLoading: true, result: null, error: null, executionTime: 0 })
    
    const startTime = performance.now()
    const response = await DatabaseAPI.textSearch(
      textSearchModal.tableName,
      textSearchModal.query,
      textSearchModal.k,
      textSearchModal.fields
    )
    const endTime = performance.now()
    
    if (response.success && response.data) {
      setQueryState({
        isLoading: false,
        result: {
          ...response.data,
          query_type: 'text_search',
          execution_time: (endTime - startTime) / 1000,
        },
        error: null,
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('success', `Búsqueda textual completada: ${response.data.count} resultados`)
      setTextSearchModal({ ...textSearchModal, isOpen: false })
    } else {
      setQueryState({
        isLoading: false,
        result: null,
        error: response.message || "Error en búsqueda textual",
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('error', response.message || 'Error en búsqueda textual')
    }
  }

  // Función para búsqueda multimedia
  const executeMultimediaSearch = async () => {
    if (!multimediaSearchModal.tableName || !multimediaSearchModal.queryFilePath.trim()) {
      addNotification('error', 'Selecciona una tabla y un archivo de consulta')
      return
    }

    setQueryState({ isLoading: true, result: null, error: null, executionTime: 0 })
    
    const startTime = performance.now()
    const response = await DatabaseAPI.multimediaSearch(
      multimediaSearchModal.tableName,
      multimediaSearchModal.queryFilePath,
      multimediaSearchModal.k,
      multimediaSearchModal.fields,
      multimediaSearchModal.method
    )
    const endTime = performance.now()
    
    if (response.success && response.data) {
      setQueryState({
        isLoading: false,
        result: {
          ...response.data,
          query_type: 'multimedia_search',
          execution_time: (endTime - startTime) / 1000,
        },
        error: null,
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('success', `Búsqueda multimedia completada: ${response.data.count} resultados`)
      setMultimediaSearchModal({ ...multimediaSearchModal, isOpen: false })
    } else {
      setQueryState({
        isLoading: false,
        result: null,
        error: response.message || "Error en búsqueda multimedia",
        executionTime: (endTime - startTime) / 1000
      })
      
      addNotification('error', response.message || 'Error en búsqueda multimedia')
    }
  }

  // Funciones para vista expandida en línea
  const toggleExpandedMedia = (type: 'image' | 'audio', path: string, title: string, index: number) => {
    if (expandedMedia.index === index && expandedMedia.type === type) {
      // Si ya está expandido, colapsarlo
      setExpandedMedia({ type: null, path: '', title: '', index: -1 })
    } else {
      // Expandir el nuevo elemento
      setExpandedMedia({ type, path, title, index })
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

  const handlePickleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    if (!file.name.endsWith('.pkl')) {
      addNotification('error', 'Solo se permiten archivos pickle (.pkl)')
      return
    }

    // Primero subir el archivo
    const formData = new FormData()
    formData.append('file', file)

    try {
      const uploadResponse = await fetch('http://localhost:8000/tables/upload-pickle-file', {
        method: 'POST',
        body: formData
      })
      
      if (!uploadResponse.ok) {
        const errorData = await uploadResponse.json()
        throw new Error(errorData.detail || 'Error al subir archivo pickle')
      }
      
      const uploadData = await uploadResponse.json()
      
      // Luego cargar la tabla desde el pickle
      // Extraer el nombre base sin _histograms, _codebook o _features
      let tableName = file.name.replace('.pkl', '')
      tableName = tableName.replace(/_histograms$/, '')
                          .replace(/_codebook$/, '')
                          .replace(/_features$/, '')
                          .replace(/[^a-zA-Z0-9_]/g, '_')
      
      const loadResponse = await DatabaseAPI.loadPickleTable({
        table_name: tableName,
        pickle_file_path: uploadData.data.file_path
      })
      
      if (loadResponse.success) {
        addNotification('success', `Tabla "${tableName}" cargada exitosamente desde pickle`)
        loadTables()
      } else {
        addNotification('error', loadResponse.message || 'Error al cargar tabla desde pickle')
      }
    } catch (error) {
      addNotification('error', error instanceof Error ? error.message : 'Error al procesar archivo pickle')
    }
  }

  const clearQuery = () => {
    setSqlQuery("")
    setQueryState({ isLoading: false, result: null, error: null, executionTime: 0 })
    setCurrentPage(1)
    setTotalRecords(0)
  }

  // NUEVO: Funciones para manejar campos de texto
  const addTextField = () => {
    const fieldName = (document.getElementById('newTextField') as HTMLInputElement)?.value?.trim()
    if (fieldName && !createTextModal.textFields.includes(fieldName)) {
      setCreateTextModal({
        ...createTextModal,
        textFields: [...createTextModal.textFields, fieldName]
      });
      (document.getElementById('newTextField') as HTMLInputElement).value = ''
    }
  }

  const removeTextField = (field: string) => {
    setCreateTextModal({
      ...createTextModal,
      textFields: createTextModal.textFields.filter(f => f !== field)
    })
  }

  const addSearchField = () => {
    const fieldName = (document.getElementById('newSearchField') as HTMLInputElement)?.value?.trim()
    if (fieldName && !textSearchModal.fields.includes(fieldName)) {
      setTextSearchModal({
        ...textSearchModal,
        fields: textSearchModal.fields.filter(f => f !== '*').concat(fieldName)
      });
      (document.getElementById('newSearchField') as HTMLInputElement).value = ''
    }
  }

  const removeSearchField = (field: string) => {
    const newFields = textSearchModal.fields.filter(f => f !== field)
    setTextSearchModal({
      ...textSearchModal,
      fields: newFields.length === 0 ? ['*'] : newFields
    })
  }

  // ========== RENDER HELPERS ==========

  const getIndexIcon = (indexType: string) => {
    const icons: Record<string, React.ComponentType<{ className?: string }>> = {
      'sequential': FileText,
      'isam': Building,
      'hash': Hash,
      'btree': TreePine,
      'rtree': MapPin,
      'SPIMI': BookOpen,
      'textual': MessageSquare
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

  const renderMultimediaResults = (columns: string[], paginatedRows: any[][], startIndex: number, endIndex: number) => {
    const imagePathIndex = columns.findIndex(col => col.includes('image_path'));
    const audioPathIndex = columns.findIndex(col => col.includes('audio_path'));
    const filenameIndex = columns.findIndex(col => col.includes('filename'));
    const similarityIndex = columns.findIndex(col => col.includes('similarity') || col.includes('distance'));
    
    // Determinar tipo de búsqueda basado en la consulta SQL o estructura de datos
    const lastQuery = sqlQuery || '';
    const queryFileMatch = lastQuery.match(/WHERE\s+\w+\s+<->\s+["']([^"']+)["']/i);
    const queryFilePath = queryFileMatch ? queryFileMatch[1] : '';
    
    // Determinar tipo de búsqueda
    let isImageSearch = imagePathIndex !== -1;
    let isAudioSearch = audioPathIndex !== -1;
    
    // Si no tenemos columnas de path definidas, inferir del contenido
    if (!isImageSearch && !isAudioSearch) {
      // Inferir del archivo de consulta
      if (queryFilePath) {
        isImageSearch = /\.(jpg|jpeg|png|gif|bmp)$/i.test(queryFilePath);
        isAudioSearch = /\.(mp3|wav|m4a|flac)$/i.test(queryFilePath);
      }
      // Si aún no sabemos, inferir del primer filename o image_path disponible
      if (!isImageSearch && !isAudioSearch && paginatedRows.length > 0) {
        const firstRow = paginatedRows[0];
        if (imagePathIndex !== -1 && firstRow[imagePathIndex]) {
          isImageSearch = true;
        } else if (audioPathIndex !== -1 && firstRow[audioPathIndex]) {
          isAudioSearch = true;
        } else if (filenameIndex !== -1) {
          const firstFilename = firstRow[filenameIndex];
          isImageSearch = /\.(jpg|jpeg|png|gif|bmp)$/i.test(firstFilename);
          isAudioSearch = /\.(mp3|wav|m4a|flac)$/i.test(firstFilename);
        }
      }
    }

    return (
      <div className="flex flex-col h-full">
        <div className="flex-1 overflow-auto p-4">
          {/* Header con información de búsqueda y archivo de consulta */}
          <div className="mb-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
            <div className="flex items-start gap-4">
              <div className="flex-1">
                <div className="flex items-center gap-3 mb-2">
                  {isImageSearch ? <Image className="w-5 h-5 text-blue-600" /> : <Music className="w-5 h-5 text-blue-600" />}
                  <h3 className="text-lg font-semibold text-blue-800">
                    Resultados de búsqueda {isImageSearch ? 'de imágenes' : 'de audio'}
                  </h3>
                </div>
                <p className="text-blue-700 text-sm">
                  {paginatedRows.length} resultados mostrados, ordenados por similitud
                </p>
              </div>
              
              {/* Preview del archivo de consulta */}
              {queryFilePath && (
                <div className="flex-shrink-0">
                  <p className="text-xs font-medium text-blue-700 mb-2">Archivo de consulta:</p>
                  {isImageSearch ? (
                    <div className="w-24 h-24 bg-white rounded border border-blue-200">
                      <img 
                        src={`/${queryFilePath}`}
                        alt="Consulta"
                        className="w-full h-full object-contain rounded"
                        onError={(e) => {
                          e.currentTarget.style.display = 'none';
                          (e.currentTarget.nextElementSibling as HTMLElement).style.display = 'flex';

                        }}
                      />
                      <div style={{display: 'none'}} className="w-full h-full flex flex-col items-center justify-center text-blue-400">
                        <Image className="w-6 h-6 mb-1" />
                        <span className="text-xs">No disponible</span>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-white p-2 rounded border border-blue-200 max-w-48">
                      <div className="flex items-center gap-2 mb-1">
                        <Music className="w-4 h-4 text-blue-600" />
                        <span className="text-xs text-blue-700 font-medium">Archivo consultado</span>
                      </div>
                      <audio controls className="w-full h-8" style={{height: '32px'}}>
                        <source src={`/${queryFilePath}`} />
                        Audio no disponible
                      </audio>
                    </div>
                  )}
                  <p className="text-xs text-blue-600 mt-1 break-all max-w-48">{queryFilePath}</p>
                </div>
              )}
            </div>
          </div>

          {/* Lista de resultados multimedia */}
          <div className="space-y-4">
            {paginatedRows.map((row, rowIndex) => {
              // Obtener la ruta del archivo multimedia
              let mediaPath = '';
              
              // Priorizar rutas completas disponibles
              if (imagePathIndex !== -1 && row[imagePathIndex]) {
                mediaPath = row[imagePathIndex];
                isImageSearch = true;
              } else if (audioPathIndex !== -1 && row[audioPathIndex]) {
                mediaPath = row[audioPathIndex];
                isAudioSearch = true;
              } else if (filenameIndex !== -1) {
                const filename = row[filenameIndex];
                // Construir ruta basada en el tipo de archivo inferido
                if (isImageSearch) {
                  mediaPath = `datos/fashion-dataset/images/${filename}`;
                } else if (isAudioSearch) {
                  mediaPath = `datos/fma_medium/000/${filename}`;
                } else {
                  mediaPath = filename; // fallback
                }
              }
              
              const similarity = similarityIndex !== -1 ? row[similarityIndex] : null;
              const currentIndex = startIndex + rowIndex;
              const isExpanded = expandedMedia.index === currentIndex;
              
              return (
                <div key={currentIndex} className="bg-white rounded-lg shadow-md border border-slate-200 overflow-hidden">
                  <div className="p-4">
                    <div className="flex items-start gap-4">
                      {/* Thumbnail/Preview pequeño */}
                      <div className="flex-shrink-0">
                        {isImageSearch ? (
                          <div className="w-20 h-20 bg-slate-100 rounded border flex items-center justify-center">
                            <img 
                              src={`/${mediaPath}`} 
                              alt="Thumbnail"
                              className="max-w-full max-h-full object-contain rounded"
                              onError={(e) => {
                                e.currentTarget.style.display = 'none';
                                (e.currentTarget.nextElementSibling as HTMLElement).style.display = 'flex';

                              }}
                            />
                            <div style={{display: 'none'}} className="flex flex-col items-center justify-center text-slate-400">
                              <Image className="w-6 h-6" />
                            </div>
                          </div>
                        ) : (
                          <div className="w-20 h-20 bg-slate-100 rounded border flex items-center justify-center">
                            <Music className="w-8 h-8 text-slate-400" />
                          </div>
                        )}
                      </div>

                      {/* Metadata */}
                      <div className="flex-1 min-w-0">
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 mb-3">
                          {columns.map((column, colIndex) => {
                            // Skip path columns, filename and similarity as they're shown elsewhere
                            if (colIndex === imagePathIndex || colIndex === audioPathIndex || colIndex === filenameIndex || colIndex === similarityIndex) {
                              return null;
                            }

                            const value = row[colIndex];
                            if (!value || value === '') return null;

                            return (
                              <div key={colIndex} className="min-w-0">
                                <span className="text-xs font-medium text-slate-500 uppercase tracking-wide block">
                                  {column}:
                                </span>
                                <span className="text-sm text-slate-800 block truncate">
                                  {String(value)}
                                </span>
                              </div>
                            );
                          })}
                        </div>
                        
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-3">
                            {similarity !== null && (
                              <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs font-medium rounded-full">
                                {(parseFloat(similarity) * 100).toFixed(1)}% similar
                              </span>
                            )}
                            <span className="text-xs text-slate-400">
                              📁 {filenameIndex !== -1 ? row[filenameIndex] : mediaPath}
                            </span>
                          </div>
                          
                          <button
                            onClick={() => {
                              const title = row[columns.findIndex(col => col.includes('productDisplayName') || col.includes('title') || col.includes('track_name'))] || 
                                          (isImageSearch ? 'Imagen' : 'Audio');
                              toggleExpandedMedia(isImageSearch ? 'image' : 'audio', mediaPath, String(title), currentIndex);
                            }}
                            className={`flex items-center gap-2 px-3 py-1 rounded-lg text-sm font-medium transition-colors ${
                              isExpanded 
                                ? 'bg-slate-200 text-slate-700 hover:bg-slate-300' 
                                : (isImageSearch ? 'bg-blue-600 text-white hover:bg-blue-700' : 'bg-green-600 text-white hover:bg-green-700')
                            }`}
                          >
                            {isImageSearch ? <Camera className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                            {isExpanded ? 'Ocultar' : (isImageSearch ? 'Ver imagen' : 'Reproducir')}
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* Vista expandida en línea */}
                    {isExpanded && (
                      <div className="mt-4 pt-4 border-t border-slate-200">
                        {isImageSearch ? (
                          <div className="max-w-2xl mx-auto">
                            <img 
                              src={`/${mediaPath}`}
                              alt={expandedMedia.title}
                              className="w-full h-auto max-h-96 object-contain rounded-lg shadow-lg"
                              onError={(e) => {
                                e.currentTarget.style.display = 'none';
                                (e.currentTarget.nextElementSibling as HTMLElement).style.display = 'flex';

                              }}
                            />
                            <div style={{display: 'none'}} className="w-full h-48 flex flex-col items-center justify-center text-slate-400 bg-slate-100 rounded-lg">
                              <Image className="w-12 h-12 mb-2" />
                              <span>Imagen no disponible</span>
                            </div>
                          </div>
                        ) : (
                          <div className="max-w-md mx-auto p-4 bg-slate-50 rounded-lg">
                            <div className="flex items-center gap-3 mb-3">
                              <Headphones className="w-6 h-6 text-slate-600" />
                              <span className="font-medium text-slate-700">{expandedMedia.title}</span>
                            </div>
                            <audio 
                              controls 
                              className="w-full"
                            >
                              <source src={`/${mediaPath}`} type="audio/mpeg" />
                              <source src={`/${mediaPath}`} type="audio/wav" />
                              <source src={`/${mediaPath}`} type="audio/mp4" />
                              Tu navegador no soporta el elemento de audio.
                            </audio>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
        
        {renderPaginationControls()}
      </div>
    );
  };

  const renderResultTable = () => {
    if (!queryState.result) {
      return null;
    }

    const columns = queryState.result.columns || []
    const allRows: Array<any[]> = 'rows' in queryState.result ? queryState.result.rows || [] : []

    if (allRows.length === 0) {
      return (
        <div className="flex items-center justify-center h-full">
          <div className="text-center text-slate-500">
            <AlertCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p className="text-lg">No se encontraron resultados</p>
          </div>
        </div>
      )
    }

    // Detectar si es resultado multimedia
    const isMultimediaResult = ('query_type' in (queryState.result || {}) && (queryState.result as QueryResult).query_type === 'multimedia_search') || 
                              columns.some(col => col.includes('image_path') || col.includes('audio_path') || col.includes('filename')) ||
                              sqlQuery.includes('<->');

    // Aplicar paginación
    const startIndex = (currentPage - 1) * recordsPerPage
    const endIndex = startIndex + recordsPerPage
    const paginatedRows = allRows.slice(startIndex, endIndex)

    // Renderizar vista especial para multimedia
    if (isMultimediaResult) {
      return renderMultimediaResults(columns, paginatedRows, startIndex, endIndex);
    }

    return (
      <div className="flex flex-col h-full">
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
                  {row.map((cell, cellIndex) => {
                    const columnName = queryState.result?.columns?.[cellIndex];
                    const isImagePath = columnName === 'image_path' || columnName === 'image';
                    
                    return (
                      <td key={cellIndex} className="px-6 py-4 text-sm text-slate-700">
                        {isImagePath && cell ? (
                          <img 
                            src={`http://localhost:8000/${cell}`}
                            alt="Imagen"
                            className="max-w-[100px] h-auto rounded"
                            onError={(e) => {
                              const target = e.target as HTMLImageElement;
                              target.style.display = 'none';
                              const span = document.createElement('span');
                              span.textContent = 'Imagen no disponible';
                              span.className = 'text-gray-500';
                              target.parentNode?.appendChild(span);
                            }}
                          />
                        ) : typeof cell === 'number' && !isNaN(cell) ? (
                          <span className="font-mono">{cell}</span>
                        ) : (
                          String(cell || '')
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        {renderPaginationControls()}
      </div>
    )
  }

  const filteredTables = tables.filter(table => 
    table.table_name.toLowerCase().includes(searchTerm.toLowerCase())
  )

  // NUEVO: Filtrar tablas textuales
  const textualTables = filteredTables.filter(table => 
    table.index_type === 'SPIMI' || table.index_type === 'textual'
  )

  // NUEVO: Función para insertar consulta textual de ejemplo
  const insertTextSearchQuery = (tableName: string) => {
    const exampleQueries = [
      `SELECT * FROM ${tableName} WHERE lyrics @@ "love song" LIMIT 10;`,
      `SELECT track_name, track_artist FROM ${tableName} WHERE lyrics @@ "rock music" LIMIT 5;`,
      `SELECT * FROM ${tableName} WHERE combined_text @@ "dance party" LIMIT 8;`
    ]
    
    const randomQuery = exampleQueries[Math.floor(Math.random() * exampleQueries.length)]
    setSqlQuery(randomQuery)
  }

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
          
          <div className="grid grid-cols-2 gap-2">
            <button
              onClick={() => setCreateModal({ ...createModal, isOpen: true })}
              className="flex items-center justify-center gap-2 px-3 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors text-sm"
            >
              <Plus className="w-4 h-4" />
              Tabla
            </button>
          </div>


          <div className="flex gap-2">
            <button
              onClick={() => fileInputRef.current?.click()}
              className="flex items-center justify-center gap-2 px-3 py-2 border border-slate-200 text-slate-700 rounded-lg hover:bg-slate-50 transition-colors"
              title="Subir CSV"
            >
              <Upload className="w-4 h-4" />
              <span className="text-xs">CSV</span>
            </button>
            
            <button
              onClick={() => pickleInputRef.current?.click()}
              className="flex items-center justify-center gap-2 px-3 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 transition-colors"
              title="Subir Pickle"
            >
              <Upload className="w-4 h-4" />
              <span className="text-xs font-bold">PKL</span>
            </button>
            
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileUpload}
              className="hidden"
            />
            
            <input
              ref={pickleInputRef}
              type="file"
              accept=".pkl"
              onChange={handlePickleUpload}
              className="hidden"
            />
          </div>

          {tables.length === 0 ? (
            <div className="p-3 bg-slate-50 rounded-lg border border-slate-200">
              <p className="text-xs text-slate-600 mb-2">Panel limpio - Solo verás las tablas que crees</p>
              <button
                onClick={loadTables}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 text-xs bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 transition-colors"
              >
                <Search className="w-3 h-3" />
                Cargar Tablas del Servidor
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
                title="Limpiar vista"
              >
                <X className="w-3 h-3" />
                Limpiar
              </button>
            </div>
          )}
        </div>

        {/* Tables */}
        <div className="px-4 pb-4">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-semibold text-slate-700">
                Mis Tablas
              </h3>
            </div>
            <button
              onClick={loadTables}
              className="p-2 hover:bg-slate-100 rounded-lg transition-colors group"
              title="Sincronizar con servidor"
            >
              <RotateCcw className="w-4 h-4 text-slate-500 group-hover:text-blue-500 transition-colors" />
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
                  <div className="font-medium truncate">{table.table_name}</div>
                </div>
              </button>
            ))}
          </div>
          
          {filteredTables.length === 0 && (
            <div className="text-center py-6 px-4 bg-slate-50 rounded-xl border-2 border-dashed border-slate-200">
              <Database className="w-10 h-10 mx-auto mb-3 text-slate-400" />
              <h4 className="text-sm font-medium text-slate-700 mb-1">¡Comienza aquí!</h4>
              <p className="text-xs text-slate-500 mb-4">Crea tu primera tabla para comenzar</p>
              <div className="space-y-2">
                <button
                  onClick={() => setCreateModal({ ...createModal, isOpen: true })}
                  className="flex items-center justify-center gap-2 w-full px-3 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors text-sm"
                >
                  <Plus className="w-4 h-4" />
                  Tabla Tradicional
                </button>
              </div>
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
          <div className="mb-6">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-xl font-bold text-slate-800">Editor SQL</h2>
              {selectedTable && (
                <div className="flex items-center gap-2 px-3 py-1 bg-blue-50 border border-blue-200 rounded-lg">
                  <Database className="w-4 h-4 text-blue-600" />
                  <span className="text-sm font-medium text-blue-700">{selectedTable}</span>
                </div>
              )}
            </div>
            <p className="text-slate-600 text-sm">
              Ejecuta consultas SQL tradicionales, búsquedas textuales con{" "}
              <code className="bg-slate-100 px-2 py-1 rounded text-purple-600 font-medium">@@</code>, o{" "}
              búsquedas multimedia con{" "}
              <code className="bg-slate-100 px-2 py-1 rounded text-blue-600 font-medium">&lt;-&gt;</code>
            </p>
          </div>
          
          <div className="relative">
            <textarea
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
              onKeyDown={(e) => {
                // Prevenir ejecución automática con Enter
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                }
              }}
              className="w-full h-32 p-4 border border-slate-200 rounded-xl font-mono text-sm bg-slate-50/50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none shadow-sm"
              placeholder="Escribe tu consulta SQL aquí y presiona 'Ejecutar' para ejecutarla...&#10;&#10;Ejemplos:&#10;• SELECT * FROM fashion WHERE image_sim &lt;-&gt; 'ruta/imagen.jpg' LIMIT 5;&#10;• SELECT * FROM fma WHERE audio_sim &lt;-&gt; 'ruta/audio.mp3' LIMIT 5;&#10;• CREATE MULTIMEDIA TABLE fashion FROM FILE 'datos/fashion_demo_100.csv' USING image WITH METHOD sift CLUSTERS 64;&#10;• CREATE MULTIMEDIA TABLE fma FROM FILE 'datos/fma_demo_100.csv' USING audio WITH METHOD mfcc CLUSTERS 64;"
            />
            <div className="absolute bottom-3 right-3 flex gap-1">
              <span className="px-2 py-1 bg-slate-200 text-slate-600 text-xs rounded-md font-mono">SQL</span>
              {sqlQuery.includes('@@') && (
                <span className="px-2 py-1 bg-purple-200 text-purple-700 text-xs rounded-md font-mono">TEXTUAL</span>
              )}
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
          <div className="mb-6 bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200/60 px-6 py-4 rounded-xl backdrop-blur-sm">
            <div className="flex justify-between items-center">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <div className={`w-3 h-3 rounded-full ${getConnectionStatusColor()}`}></div>
                  <span className="text-blue-700 font-medium text-sm">
                    {isConnected ? getConnectionStatusText() : 'Desconectado'}
                  </span>
                </div>
                
                {selectedTable && (
                  <div className="flex items-center gap-2 px-3 py-1 bg-white/60 rounded-lg border border-blue-200">
                    <Database className="w-4 h-4 text-blue-600" />
                    <span className="text-blue-700 font-medium text-sm">{selectedTable}</span>
                  </div>
                )}
                
                {queryState.result && totalRecords > 0 && (
                  <div className="flex items-center gap-2 px-3 py-1 bg-white/60 rounded-lg border border-blue-200">
                    <Grid3X3 className="w-4 h-4 text-blue-600" />
                    <span className="text-blue-700 text-sm">
                      Página {currentPage} de {totalPages} • {totalRecords.toLocaleString()} total
                    </span>
                  </div>
                )}
              </div>
              
              <div className="flex items-center gap-3">
                {queryState.executionTime > 0 && (
                  <div className="flex items-center gap-2 px-3 py-1 bg-white/60 rounded-lg border border-blue-200">
                    <span className="text-blue-700 text-sm font-medium">
                      {DatabaseAPI.formatExecutionTime(queryState.executionTime)}
                    </span>
                  </div>
                )}
                <div className={`px-3 py-1 rounded-lg font-medium text-sm ${
                  isConnected 
                    ? 'bg-green-50 border border-green-200 text-green-700' 
                    : 'bg-red-50 border border-red-200 text-red-700'
                }`}>
                  {isConnected ? 'Conectado' : 'Desconectado'}
                </div>
              </div>
            </div>
          </div>

          {/* Results Table Container with fixed height and scroll */}
          <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-xl border border-slate-200/60 overflow-hidden flex flex-col" style={{ height: 'calc(100vh - 400px)' }}>
            <div className="p-4 border-b border-slate-200/60 bg-gradient-to-r from-slate-50 to-slate-100 flex-shrink-0">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-white rounded-lg shadow-sm">
                    <Grid3X3 className="w-5 h-5 text-slate-600" />
                  </div>
                  <div>
                    <h3 className="text-lg font-bold text-slate-800">Resultados</h3>
                    <p className="text-slate-500 text-sm">
                      {selectedTable ? (
                        <span className="flex items-center gap-2">
                          <Database className="w-3 h-3" />
                          {selectedTable}
                        </span>
                      ) : (
                        'Selecciona una tabla para comenzar'
                      )}
                    </p>
                  </div>
                </div>
                
                <div className="flex items-center gap-3">
                  {/* Status indicators */}
                  {sqlQuery.includes('@@') && (
                    <div className="px-3 py-1 bg-purple-100 border border-purple-200 rounded-lg">
                      <span className="text-purple-700 text-xs font-medium">Búsqueda Textual</span>
                    </div>
                  )}
                  
                  {queryState.error ? (
                    <div className="flex items-center gap-2 px-3 py-1 bg-red-50 border border-red-200 rounded-lg">
                      <AlertCircle className="w-4 h-4 text-red-500" />
                      <span className="text-red-600 text-sm font-medium">Error</span>
                    </div>
                  ) : queryState.result ? (
                    <div className="flex items-center gap-2 px-3 py-1 bg-green-50 border border-green-200 rounded-lg">
                      <CheckCircle className="w-4 h-4 text-green-500" />
                      <span className="text-green-600 text-sm font-medium">
                        {totalRecords.toLocaleString()} registro{totalRecords !== 1 ? 's' : ''}
                      </span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 px-3 py-1 bg-slate-50 border border-slate-200 rounded-lg">
                      <div className="w-2 h-2 bg-slate-400 rounded-full"></div>
                      <span className="text-slate-600 text-sm">Listo</span>
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
                      <p>💡 Tip: Usa "SELECT * FROM tabla" para ver todos los registros</p>
                      <p>🔍 Nuevo: Usa "SELECT * FROM tabla WHERE campo @@ 'consulta'" para búsqueda textual</p>
                      <p>🚀 Soporta índices: Sequential, ISAM, Hash, B+Tree, R-Tree, SPIMI</p>
                      <p>📊 Los resultados se paginarán automáticamente en grupos de 100 registros</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* NUEVO: Modal para crear índice textual */}

      {/* NUEVO: Modal para búsqueda textual */}

      {/* Modal para búsqueda multimedia */}

      {/* Create Table Modal (existente) */}
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
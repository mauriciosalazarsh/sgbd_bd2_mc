// lib/api.ts - VERSIÓN ACTUALIZADA CON BÚSQUEDA TEXTUAL
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// ========== TIPOS ==========

export interface TableInfo {
  table_name: string;
  index_type: string;
  record_count?: number;
  field_index?: number;
  headers?: string[];
  headers_count?: number;
  csv_path?: string;
}

export interface QueryResult {
  query_type: string;
  columns: string[];
  rows: any[][];
  execution_time: number;
  affected_rows?: number;
  count?: number;
}

export interface TableScanResult {
  columns: string[];
  rows: any[][];
  total_records: number;
  execution_time: number;
}

export interface APIResponse<T = any> {
  success: boolean;
  message: string;
  data?: T | undefined;
}

// NUEVO: Tipos para búsqueda textual
export interface TextSearchRequest {
  table_name: string;
  query: string;
  k?: number;
  fields?: string[];
}

export interface TextSearchResult {
  columns: string[];
  rows: any[][];
  count: number;
  search_time: number;
  query: string;
  table_name: string;
}

// NUEVO: Tipos para búsqueda multimedia
export interface MultimediaSearchRequest {
  table_name: string;
  query_file_path: string;
  k?: number;
  fields?: string[];
  method: string;
}

export interface MultimediaSearchResult {
  columns: string[];
  rows: any[][];
  count: number;
  search_time: number;
  query_file_path: string;
  table_name: string;
  method: string;
}

export interface CreateTextIndexRequest {
  table_name: string;
  csv_file_path: string;
  text_fields: string[];
  language?: string;
}

// ========== CLIENTE API ==========

export class DatabaseAPI {
  
  static isValidSQLQuery(query: string): boolean {
    const trimmed = query.trim().toLowerCase();
    const validStarts = ['select', 'insert', 'delete', 'create'];
    return validStarts.some(start => trimmed.startsWith(start));
  }
  
  static formatExecutionTime(seconds: number): string {
    if (seconds < 0.001) return '< 1ms';
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
    return `${seconds.toFixed(2)}s`;
  }
  
  static getIndexTypeLabel(indexType: string): string {
    const labels: Record<string, string> = {
      'sequential': 'Sequential File',
      'isam': 'ISAM',
      'hash': 'Extendible Hash',
      'bplustree': 'B+ Tree',
      'btree': 'B+ Tree',
      'rtree': 'R-Tree',
      'SPIMI': 'Índice Textual SPIMI',
      'textual': 'Índice Textual'
    };
    return labels[indexType.toLowerCase()] || indexType;
  }
  
  private static async makeRequest<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<APIResponse<T>> {
    try {
      const url = `${API_BASE_URL}${endpoint}`;
      
      console.log(`🌐 Haciendo request a: ${url}`);
      
      const defaultHeaders: Record<string, string> = {
        'Content-Type': 'application/json',
      };
      
      const response = await fetch(url, {
        ...options,
        headers: {
          ...defaultHeaders,
          ...options.headers,
        },
      });

      console.log(`📡 Response status: ${response.status}`);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      console.log(`✅ Response data:`, data);
      
      return data as APIResponse<T>;
      
    } catch (error) {
      console.error(`❌ Error en ${endpoint}:`, error);
      return {
        success: false,
        message: error instanceof Error ? error.message : 'Error desconocido',
        data: undefined
      };
    }
  }

  static async getHealth(): Promise<APIResponse<{ status: string; tables: string[] }>> {
    return this.makeRequest('/health');
  }

  static async getTables(): Promise<APIResponse<TableInfo[]>> {
    const response = await this.makeRequest<{ tables: Record<string, any>; count: number }>('/tables');
    
    if (response.success && response.data) {
      const tables: TableInfo[] = Object.entries(response.data.tables).map(([tableName, info]) => ({
        table_name: tableName,
        index_type: this.normalizeIndexType(info.index_type || info.type || 'unknown'),
        field_index: info.field_index,
        record_count: info.record_count || 0,
        headers: info.headers || [],
        headers_count: info.headers_count || 0,
        csv_path: info.csv_path || ''
      }));
      
      return {
        success: true,
        message: response.message,
        data: tables
      };
    }
    
    return {
      success: response.success,
      message: response.message,
      data: []
    };
  }

  // ========== MÉTODO PRINCIPAL CORREGIDO ==========
  static async executeSQL(query: string): Promise<APIResponse<QueryResult | TableScanResult>> {
    const requestBody = { query: query.trim() };
    
    console.log('🔍 Enviando consulta:', requestBody);
    
    const response = await this.makeRequest<{
      columns: string[];
      rows: any[][];
      count: number;
      query: string;
      table_name?: string;
      csv_path?: string;
      execution_time?: number;
      query_type?: string;
    }>('/sql/execute', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });

    console.log('📥 Respuesta del backend:', response);

    if (response.success && response.data) {
      const result: QueryResult = {
        query_type: response.data.query_type || this.getQueryType(query),
        columns: response.data.columns || [],
        rows: response.data.rows || [],
        execution_time: response.data.execution_time || 0,
        count: response.data.count || (response.data.rows ? response.data.rows.length : 0)
      };
      
      console.log('✅ Resultado parseado:', result);
      
      return {
        success: true,
        message: response.message,
        data: result
      };
    }

    console.log('❌ Error en respuesta:', response);

    return {
      success: response.success,
      message: response.message,
      data: {
        query_type: this.getQueryType(query),
        columns: [],
        rows: [],
        execution_time: 0,
        count: 0
      }
    };
  }

  static async scanTable(tableName: string, limit: number = 100): Promise<APIResponse<TableScanResult>> {
    const response = await this.makeRequest<{
      columns: string[];
      rows: any[][];
      total_records: number;
      table_name?: string;
      csv_path?: string;
    }>(`/tables/${tableName}/scan`);

    console.log('📥 Scan response:', response);

    if (response.success && response.data) {
      const limitedRows = response.data.rows.slice(0, limit);
      
      const result: TableScanResult = {
        columns: response.data.columns || [],
        rows: limitedRows,
        total_records: response.data.total_records || 0,
        execution_time: 0
      };
      
      return {
        success: true,
        message: `Se escanearon ${limitedRows.length} registros de la tabla ${tableName}`,
        data: result
      };
    }

    return {
      success: response.success,
      message: response.message,
      data: {
        columns: [],
        rows: [],
        total_records: 0,
        execution_time: 0
      }
    };
  }

  // ========== NUEVOS MÉTODOS PARA BÚSQUEDA TEXTUAL ==========

  static async createTextIndex(
    tableName: string,
    csvFilePath: string,
    textFields: string[],
    language: string = 'spanish'
  ): Promise<APIResponse<{ table_name: string; index_type: string; text_fields: string[] }>> {
    const requestBody: CreateTextIndexRequest = {
      table_name: tableName,
      csv_file_path: csvFilePath,
      text_fields: textFields,
      language: language
    };

    console.log('🔍 Creando índice textual:', requestBody);

    return this.makeRequest('/tables/create-text-index', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  static async textSearch(
    tableName: string,
    query: string,
    k: number = 10,
    fields: string[] = ['*']
  ): Promise<APIResponse<TextSearchResult>> {
    const requestBody: TextSearchRequest = {
      table_name: tableName,
      query: query,
      k: k,
      fields: fields
    };

    console.log('🔍 Ejecutando búsqueda textual:', requestBody);

    const response = await this.makeRequest<{
      columns: string[];
      rows: any[][];
      count: number;
      search_time: number;
      query: string;
      table_name: string;
      execution_time?: number;
    }>('/search/text', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });

    if (response.success && response.data) {
      const result: TextSearchResult = {
        columns: response.data.columns || [],
        rows: response.data.rows || [],
        count: response.data.count || 0,
        search_time: response.data.search_time || response.data.execution_time || 0,
        query: response.data.query || query,
        table_name: response.data.table_name || tableName
      };

      return {
        success: true,
        message: response.message || `Búsqueda textual completada: ${result.count} resultados`,
        data: result
      };
    }

    return {
      success: response.success,
      message: response.message || 'Error en búsqueda textual',
      data: {
        columns: [],
        rows: [],
        count: 0,
        search_time: 0,
        query: query,
        table_name: tableName
      }
    };
  }

  static async multimediaSearch(
    tableName: string,
    queryFilePath: string,
    k: number = 10,
    fields: string[] = ['*'],
    method: string = 'SIFT'
  ): Promise<APIResponse<MultimediaSearchResult>> {
    const requestBody: MultimediaSearchRequest = {
      table_name: tableName,
      query_file_path: queryFilePath,
      k: k,
      fields: fields,
      method: method
    };

    console.log('🔍 Ejecutando búsqueda multimedia:', requestBody);

    const response = await this.makeRequest<{
      columns: string[];
      rows: any[][];
      count: number;
      search_time: number;
      query_file_path: string;
      table_name: string;
      method: string;
      execution_time?: number;
    }>('/multimedia/search', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });

    if (response.success && response.data) {
      const result: MultimediaSearchResult = {
        columns: response.data.columns || [],
        rows: response.data.rows || [],
        count: response.data.count || 0,
        search_time: response.data.search_time || response.data.execution_time || 0,
        query_file_path: response.data.query_file_path || queryFilePath,
        table_name: response.data.table_name || tableName,
        method: response.data.method || method
      };

      return {
        success: true,
        message: response.message || `Búsqueda multimedia completada: ${result.count} resultados`,
        data: result
      };
    }

    return {
      success: response.success,
      message: response.message || 'Error en búsqueda multimedia',
      data: {
        columns: [],
        rows: [],
        count: 0,
        search_time: 0,
        query_file_path: queryFilePath,
        table_name: tableName,
        method: method
      }
    };
  }

  // ========== MÉTODOS EXISTENTES ==========

  static async createTable(
    tableName: string,
    csvFilePath: string,
    indexType: string,
    indexField: string
  ): Promise<APIResponse<{ table_name: string; index_type: string; headers?: string[] }>> {
    const requestBody = {
      table_name: tableName,
      csv_file_path: csvFilePath,
      index_type: this.mapIndexType(indexType),
      index_field: parseInt(indexField) || 0
    };

    return this.makeRequest('/tables/create', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  static async uploadCSV(file: File): Promise<APIResponse<{
    file_path: string;
    headers: string[];
    preview: string[][];
    columns_count: number;
  }>> {
    const formData = new FormData();
    formData.append('file', file);

    return this.makeRequest('/tables/upload-csv', {
      method: 'POST',
      headers: {}, // No establecer Content-Type para FormData
      body: formData,
    });
  }

  static async loadPickleTable(params: {
    table_name: string;
    pickle_file_path: string;
    index_type?: string;
  }): Promise<APIResponse<{
    table_name: string;
    pickle_path: string;
    headers: string[];
    record_count: number;
    data_type: string;
  }>> {
    const requestBody = {
      table_name: params.table_name,
      pickle_file_path: params.pickle_file_path,
      index_type: params.index_type || 'sequential'
    };

    return this.makeRequest('/tables/load-pickle', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  static async insertRecord(tableName: string, values: string[]): Promise<APIResponse<{ inserted_values: string[] }>> {
    const requestBody = {
      table_name: tableName,
      values: values
    };

    return this.makeRequest('/records/insert', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  static async searchRecords(tableName: string, key: string, column: number): Promise<APIResponse<{
    records: string[];
    count: number;
  }>> {
    const requestBody = {
      table_name: tableName,
      key: key,
      column: column
    };

    return this.makeRequest('/records/search', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  static async deleteRecords(tableName: string, key: string): Promise<APIResponse<{
    deleted_records: string[];
    count: number;
  }>> {
    const requestBody = {
      table_name: tableName,
      key: key
    };

    return this.makeRequest('/records/delete', {
      method: 'DELETE',
      body: JSON.stringify(requestBody),
    });
  }

  static async getTableInfo(tableName: string): Promise<APIResponse<{
    name: string;
    index_type: string;
    field_index: number;
    sample_records: string[];
    total_records: number;
    headers?: string[];
    csv_path?: string;
  }>> {
    return this.makeRequest(`/tables/${tableName}/info`);
  }

  static async getTableHeaders(tableName: string): Promise<APIResponse<{
    headers: string[];
    count: number;
    table_name: string;
    csv_path: string;
  }>> {
    return this.makeRequest(`/tables/${tableName}/headers`);
  }

  // ========== HELPERS PRIVADOS ==========

  private static normalizeIndexType(backendType: string): string {
    const mapping: Record<string, string> = {
      'SequentialFile': 'sequential',
      'ISAM': 'isam',
      'ExtendibleHash': 'hash',
      'BPlusTree': 'btree',
      'RTree': 'rtree',
      'SPIMI': 'SPIMI',
      'textual': 'textual',
      'SpotifyTextual': 'SPIMI', // En caso de que el backend use nombres específicos
      'TextIndex': 'textual'
    };
    
    return mapping[backendType] || backendType.toLowerCase();
  }

  private static mapIndexType(frontendType: string): string {
    const mapping: Record<string, string> = {
      'sequential': 'sequential',
      'isam': 'isam',
      'hash': 'hash',
      'btree': 'bplustree',
      'rtree': 'rtree',
      'SPIMI': 'spimi',
      'textual': 'spimi'
    };
    
    return mapping[frontendType] || frontendType;
  }

  private static getQueryType(query: string): string {
    const trimmed = query.trim().toLowerCase();
    if (trimmed.startsWith('select')) {
      if (trimmed.includes('@@')) {
        return 'TEXT_SEARCH';
      }
      return 'SELECT';
    }
    if (trimmed.startsWith('insert')) return 'INSERT';
    if (trimmed.startsWith('delete')) return 'DELETE';
    if (trimmed.startsWith('create')) {
      if (trimmed.includes('spimi') || trimmed.includes('text')) {
        return 'CREATE_TEXT_INDEX';
      }
      return 'CREATE_TABLE';
    }
    return 'UNKNOWN';
  }

  // ========== MÉTODOS DE UTILIDAD PARA BÚSQUEDA TEXTUAL ==========

  static generateTextSearchExamples(tableName: string): string[] {
    return [
      `SELECT * FROM ${tableName} WHERE lyrics @@ 'love song' LIMIT 10;`,
      `SELECT track_name, track_artist FROM ${tableName} WHERE lyrics @@ 'rock music' LIMIT 5;`,
      `SELECT * FROM ${tableName} WHERE combined_text @@ 'dance party' LIMIT 8;`,
      `SELECT title, artist FROM ${tableName} WHERE content @@ 'acoustic guitar' LIMIT 6;`
    ];
  }

  static validateTextSearchQuery(query: string): { isValid: boolean; error?: string } {
    const trimmed = query.trim().toLowerCase();
    
    if (!trimmed.includes('@@')) {
      return { isValid: false, error: 'La consulta debe incluir el operador @@' };
    }
    
    if (!trimmed.startsWith('select')) {
      return { isValid: false, error: 'Solo se permiten consultas SELECT para búsqueda textual' };
    }
    
    const aaMatch = query.match(/(\w+)\s*@@\s*['"]([^'"]+)['"]/i);
    if (!aaMatch) {
      return { isValid: false, error: 'Formato incorrecto. Use: campo @@ "consulta"' };
    }
    
    return { isValid: true };
  }

  static extractSearchTermsFromQuery(query: string): { field: string; terms: string } | null {
    const aaMatch = query.match(/(\w+)\s*@@\s*['"]([^'"]+)['"]/i);
    if (aaMatch) {
      return {
        field: aaMatch[1],
        terms: aaMatch[2]
      };
    }
    return null;
  }
}
// lib/api.ts (COMPLETO)
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
      'rtree': 'R-Tree'
    };
    return labels[indexType.toLowerCase()] || indexType;
  }
  
  private static async makeRequest<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<APIResponse<T>> {
    try {
      const url = `${API_BASE_URL}${endpoint}`;
      
      console.log(`🌐 Haciendo request a: ${url}`); // Debug log
      
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

      console.log(`📡 Response status: ${response.status}`); // Debug log

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      console.log(`✅ Response data:`, data); // Debug log
      
      // El backend ya devuelve el formato APIResponse correcto
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
        index_type: this.normalizeIndexType(info.index_type),
        field_index: info.field_index,
        record_count: 0,
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
    
    console.log('🔍 Enviando consulta:', requestBody); // Debug log
    
    const response = await this.makeRequest<{
      columns: string[];     // ← El backend YA devuelve este formato
      rows: any[][];         // ← El backend YA devuelve este formato
      count: number;
      query: string;
      table_name?: string;
      csv_path?: string;
    }>('/sql/execute', {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });

    console.log('📥 Respuesta del backend:', response); // Debug log

    if (response.success && response.data) {
      // El backend YA devuelve el formato correcto
      const result: QueryResult = {
        query_type: this.getQueryType(query),
        columns: response.data.columns || [],
        rows: response.data.rows || [],
        execution_time: 0
      };
      
      console.log('✅ Resultado parseado:', result); // Debug log
      
      return {
        success: true,
        message: response.message,
        data: result
      };
    }

    console.log('❌ Error en respuesta:', response); // Debug log

    return {
      success: response.success,
      message: response.message,
      data: {
        query_type: this.getQueryType(query),
        columns: [],
        rows: [],
        execution_time: 0
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

    console.log('📥 Scan response:', response); // Debug log

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
      'RTree': 'rtree'
    };
    
    return mapping[backendType] || backendType.toLowerCase();
  }

  private static mapIndexType(frontendType: string): string {
    const mapping: Record<string, string> = {
      'sequential': 'sequential',
      'isam': 'isam',
      'hash': 'hash',
      'btree': 'bplustree',
      'rtree': 'rtree'
    };
    
    return mapping[frontendType] || frontendType;
  }

  private static getQueryType(query: string): string {
    const trimmed = query.trim().toLowerCase();
    if (trimmed.startsWith('select')) return 'SELECT';
    if (trimmed.startsWith('insert')) return 'INSERT';
    if (trimmed.startsWith('delete')) return 'DELETE';
    if (trimmed.startsWith('create')) return 'CREATE';
    return 'UNKNOWN';
  }
}
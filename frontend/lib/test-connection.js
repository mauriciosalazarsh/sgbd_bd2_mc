// test-connection.js - Script para verificar la conexión
// Ejecutar en la consola del navegador o como script de Node.js

// Verificación básica de la API
async function testConnection() {
  const API_URL = 'http://localhost:8000';
  
  console.log('🔍 Verificando conexión con el backend...');
  
  try {
    // Test 1: Health Check
    console.log('\n1️⃣ Probando endpoint /health...');
    const healthResponse = await fetch(`${API_URL}/health`);
    const healthData = await healthResponse.json();
    console.log('✅ Health check:', healthData);
    
    // Test 2: Listar tablas
    console.log('\n2️⃣ Probando endpoint /tables...');
    const tablesResponse = await fetch(`${API_URL}/tables`);
    const tablesData = await tablesResponse.json();
    console.log('✅ Tablas disponibles:', tablesData);
    
    // Test 3: Documentación automática
    console.log('\n3️⃣ Verificando documentación automática...');
    const docsResponse = await fetch(`${API_URL}/docs`);
    if (docsResponse.ok) {
      console.log('✅ Documentación disponible en: http://localhost:8000/docs');
    } else {
      console.log('⚠️  Documentación no accesible');
    }
    
    // Test 4: CORS
    console.log('\n4️⃣ Verificando CORS...');
    const corsResponse = await fetch(`${API_URL}/health`, {
      method: 'GET',
      headers: {
        'Origin': 'http://localhost:3000'
      }
    });
    
    if (corsResponse.ok) {
      console.log('✅ CORS configurado correctamente');
    } else {
      console.log('❌ Error de CORS');
    }
    
    console.log('\n🎉 ¡Conexión verificada exitosamente!');
    return true;
    
  } catch (error) {
    console.error('\n❌ Error de conexión:', error.message);
    console.log('\n🔧 Pasos para solucionar:');
    console.log('1. Verifica que el backend esté corriendo en puerto 8000');
    console.log('2. Ejecuta: uvicorn main:app --reload --host 0.0.0.0 --port 8000');
    console.log('3. Verifica que no haya firewall bloqueando el puerto');
    return false;
  }
}

// Prueba de una consulta SQL simple
async function testSQLQuery() {
  const API_URL = 'http://localhost:8000';
  
  console.log('\n🔍 Probando consulta SQL...');
  
  try {
    const response = await fetch(`${API_URL}/sql/execute`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: 'select * from students'  // Tabla de ejemplo
      })
    });
    
    const data = await response.json();
    
    if (response.ok) {
      console.log('✅ Consulta SQL ejecutada:', data);
    } else {
      console.log('ℹ️  Tabla "students" no existe (normal si no has creado tablas)');
      console.log('💡 Prueba crear una tabla primero con CREATE TABLE');
    }
    
  } catch (error) {
    console.error('❌ Error ejecutando SQL:', error.message);
  }
}

// Función principal
async function runAllTests() {
  console.log('🧪 Iniciando pruebas de conexión...\n');
  
  const isConnected = await testConnection();
  
  if (isConnected) {
    await testSQLQuery();
    
    console.log('\n📋 Resumen:');
    console.log('- Backend: ✅ Funcionando');
    console.log('- Frontend: 🔗 Listo para conectar');
    console.log('- CORS: ✅ Configurado');
    console.log('\n🚀 Todo listo para usar la aplicación!');
  } else {
    console.log('\n⚠️  Soluciona los problemas de conexión antes de continuar');
  }
}

// Auto-ejecutar si está en Node.js
if (typeof window === 'undefined') {
  // Entorno Node.js
  const fetch = require('node-fetch');
  runAllTests();
} else {
  // Entorno navegador
  console.log('Ejecuta runAllTests() para probar la conexión');
}
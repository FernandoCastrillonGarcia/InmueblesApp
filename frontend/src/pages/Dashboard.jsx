import { useState, useEffect } from "react";
import { TrendingUp, Home, MapPin, Building2 } from "lucide-react";

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/stats")
      .then(r => r.json())
      .then(data => {
        setStats(data);
        setLoading(false);
      })
      .catch(e => {
        console.error(e);
        setLoading(false);
      });
  }, []);

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-700">
      <header>
        <h1 className="text-4xl font-bold text-slate-100 mb-2">Resumen del Mercado</h1>
        <p className="text-slate-400">Análisis inteligente de propiedades en Bogotá</p>
      </header>

      {loading ? (
        <div className="flex justify-center p-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500"></div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard 
            title="Total Propiedades" 
            value={(stats?.Arriendo?.total_properties || 0) + (stats?.Venta?.total_properties || 0)} 
            icon={Home}
            color="from-blue-500 to-cyan-400"
          />
          <StatCard 
            title="Precio Prom. Venta" 
            value={`$${Math.round((stats?.Venta?.avg_price || 0) / 1000000)}M`} 
            icon={TrendingUp}
            color="from-indigo-500 to-purple-500"
          />
          <StatCard 
            title="Precio Prom. Arriendo" 
            value={`$${Math.round((stats?.Arriendo?.avg_price || 0) / 1000)}K`} 
            icon={TrendingUp}
            color="from-pink-500 to-rose-500"
          />
          <StatCard 
            title="Área Promedio" 
            value={`${Math.round(stats?.Venta?.avg_area || 0)} m²`} 
            icon={MapPin}
            color="from-emerald-500 to-teal-400"
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-12">
         <div className="glass-card p-8">
            <div className="flex items-center gap-3 mb-6">
              <div className="p-3 bg-indigo-500/20 rounded-lg text-indigo-400">
                <Building2 size={24} />
              </div>
              <h2 className="text-2xl font-semibold">¿Por qué InmueblesApp?</h2>
            </div>
            <p className="text-slate-300 leading-relaxed">
              Utilizamos Inteligencia Artificial avanzada para entender el mercado inmobiliario.
              Nuestro motor de búsqueda vectorial basado en embeddings te permite buscar propiedades 
              con lenguaje natural, mientras que nuestros modelos XGBoost predicen precios con gran precisión.
            </p>
         </div>
         <div className="glass-card p-8 bg-gradient-to-br from-slate-800/60 to-indigo-900/20">
             <h2 className="text-2xl font-semibold mb-4 text-gradient">Novedades V2</h2>
             <ul className="space-y-3 text-slate-300">
               <li className="flex items-center gap-2">✨ Interfaz React ultrarrápida</li>
               <li className="flex items-center gap-2">🚀 Backend FastAPI asíncrono</li>
               <li className="flex items-center gap-2">🧠 MongoDB Vector Search integrado</li>
               <li className="flex items-center gap-2">📊 GCP Vertex AI Pipelines</li>
             </ul>
         </div>
      </div>
    </div>
  );
}

function StatCard({ title, value, icon: Icon, color }) {
  return (
    <div className="glass-card p-6 relative overflow-hidden group">
      <div className={`absolute top-0 right-0 w-32 h-32 bg-gradient-to-br ${color} opacity-10 rounded-full blur-2xl group-hover:opacity-20 transition-opacity`} />
      <div className="flex items-start justify-between relative z-10">
        <div>
          <p className="text-slate-400 text-sm font-medium mb-1">{title}</p>
          <h3 className="text-3xl font-bold text-slate-100">{value}</h3>
        </div>
        <div className={`p-3 rounded-xl bg-gradient-to-br ${color} bg-opacity-10 backdrop-blur-md`}>
          <Icon className="text-white" size={24} />
        </div>
      </div>
    </div>
  );
}

import { useState } from "react";
import { Search as SearchIcon, MapPin, Bed, Bath, Maximize } from "lucide-react";

export default function Recommend() {
  const [query, setQuery] = useState("");
  const [operation, setOperation] = useState("Arriendo");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState([]);

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!query) return;
    
    setLoading(true);
    try {
      const res = await fetch("/api/recommend", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, operation_type: operation, limit: 10 })
      });
      const data = await res.json();
      setResults(data.results || []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-700">
      <header>
        <h1 className="text-4xl font-bold text-slate-100 mb-2">Búsqueda Inteligente</h1>
        <p className="text-slate-400">Describe la propiedad de tus sueños y la IA la encontrará por ti.</p>
      </header>

      <form onSubmit={handleSearch} className="glass-card p-6">
        <div className="flex flex-col md:flex-row gap-4">
          <select 
            value={operation} 
            onChange={e => setOperation(e.target.value)}
            className="bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-500 md:w-48"
          >
            <option value="Arriendo">Arriendo</option>
            <option value="Venta">Venta</option>
          </select>
          <div className="relative flex-1">
            <input 
              type="text" 
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Ej: Apartamento moderno con balcón, cerca del metro, zona segura..."
              className="w-full bg-slate-900 border border-slate-700 rounded-xl pl-12 pr-4 py-3 text-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            />
            <SearchIcon className="absolute left-4 top-3.5 text-slate-500" size={20} />
          </div>
          <button 
            type="submit" 
            disabled={loading}
            className="bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white font-medium py-3 px-8 rounded-xl transition-all shadow-lg shadow-indigo-500/25 disabled:opacity-50"
          >
            {loading ? "Buscando..." : "Buscar"}
          </button>
        </div>
      </form>

      {results.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-8">
          {results.map((r, i) => (
            <div key={i} className="glass-card p-6 flex flex-col hover:-translate-y-1 transition-transform">
              <div className="flex justify-between items-start mb-4">
                <span className="px-3 py-1 bg-indigo-500/20 text-indigo-300 rounded-full text-xs font-semibold">
                  {r.PROPERTY_TYPE || 'Propiedad'}
                </span>
                <span className="text-2xl font-bold text-emerald-400">
                  ${r.PRICE?.toLocaleString()}
                </span>
              </div>
              <p className="text-slate-300 text-sm mb-6 flex-1 line-clamp-3">
                {r.DESCRIPTION || 'Sin descripción'}
              </p>
              
              <div className="grid grid-cols-3 gap-4 border-t border-slate-700/50 pt-4 mb-4">
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <Maximize size={16} /> {r.AREA} m²
                </div>
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <Bed size={16} /> {r.ROOMS} Hab.
                </div>
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <Bath size={16} /> {r.BATHROOMS} Baños
                </div>
              </div>
              
              <a 
                href={r.LINK?.startsWith('http') ? r.LINK : `https://${r.LINK}`} 
                target="_blank" 
                rel="noreferrer"
                className="w-full py-2 bg-slate-800 hover:bg-slate-700 text-center rounded-lg text-sm font-medium transition-colors border border-slate-700"
              >
                Ver Propiedad Original
              </a>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

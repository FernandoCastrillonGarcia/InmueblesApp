import { useState } from "react";
import { Calculator } from "lucide-react";

export default function Predict() {
  const [formData, setFormData] = useState({
    AREA: 60,
    BUILT_AREA: 60,
    PRIVATE_AREA: 60,
    LATITUDE: 4.6097,
    LONGITUDE: -74.0817,
    FLOOR: 3,
    ROOMS: 2,
    BATHROOMS: 2,
    GARAGE: 1,
    STRATUM: 3,
    BEDROOMS: 2,
    ANTIQUITY: "1 a 8 años",
    PROPERTY_TYPE: "Apartamento"
  });

  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleChange = (e) => {
    const { name, value, type } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === "number" ? parseFloat(value) : value
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData)
      });
      if (!res.ok) throw new Error("Error en la predicción. ¿Está el modelo cargado?");
      const data = await res.json();
      setPrediction(data.predicted_price);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-700">
      <header>
        <h1 className="text-4xl font-bold text-slate-100 mb-2">Predicción de Precios</h1>
        <p className="text-slate-400">Estima el valor de mercado usando nuestro modelo XGBoost entrenado.</p>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 glass-card p-8">
          <form onSubmit={handleSubmit} className="space-y-6">
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Tipo de Propiedad</label>
                <select name="PROPERTY_TYPE" value={formData.PROPERTY_TYPE} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200">
                  <option>Apartamento</option>
                  <option>Casa</option>
                  <option>Oficina</option>
                  <option>Local</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Antigüedad</label>
                <select name="ANTIQUITY" value={formData.ANTIQUITY} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200">
                  <option>Menos de 1 año</option>
                  <option>1 a 8 años</option>
                  <option>9 a 15 años</option>
                  <option>16 a 30 años</option>
                  <option>Más de 30 años</option>
                </select>
              </div>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Área (m²)</label>
                <input type="number" name="AREA" value={formData.AREA} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200" />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Estrato</label>
                <select name="STRATUM" value={formData.STRATUM} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200">
                  {[1,2,3,4,5,6].map(n => <option key={n}>{n}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Habitaciones</label>
                <input type="number" name="ROOMS" value={formData.ROOMS} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200" />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-2">Baños</label>
                <input type="number" name="BATHROOMS" value={formData.BATHROOMS} onChange={handleChange} className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-slate-200" />
              </div>
            </div>

            <button 
              type="submit" 
              disabled={loading}
              className="w-full mt-6 flex items-center justify-center gap-2 bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white font-medium py-4 px-8 rounded-xl transition-all shadow-lg shadow-indigo-500/25 disabled:opacity-50"
            >
              <Calculator size={20} />
              {loading ? "Calculando..." : "Predecir Precio"}
            </button>
          </form>
        </div>

        <div className="lg:col-span-1">
          <div className="glass-card p-8 text-center h-full flex flex-col items-center justify-center min-h-[300px]">
            <h3 className="text-xl font-medium text-slate-400 mb-4">Valor Estimado</h3>
            
            {loading && <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mb-4"></div>}
            
            {!loading && !prediction && !error && (
              <p className="text-slate-500 italic">Llena el formulario para calcular el precio.</p>
            )}

            {!loading && error && (
              <p className="text-red-400 bg-red-400/10 p-4 rounded-xl border border-red-400/20">{error}</p>
            )}

            {!loading && prediction && (
              <div className="animate-in zoom-in duration-500">
                <span className="text-5xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-emerald-400 to-teal-400">
                  ${Math.round(prediction).toLocaleString()}
                </span>
                <p className="text-emerald-500/80 mt-2 font-medium">Predicción Exitosa</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

import { Route, Switch, Link, useLocation } from "wouter";
import { Home, Search, Calculator, LayoutDashboard } from "lucide-react";
import Dashboard from "./pages/Dashboard";
import Recommend from "./pages/Recommend";
import Predict from "./pages/Predict";

function Sidebar() {
  const [location] = useLocation();

  const navItems = [
    { href: "/", icon: Home, label: "Inicio" },
    { href: "/search", icon: Search, label: "Buscar Similares" },
    { href: "/predict", icon: Calculator, label: "Predecir Precio" },
    { href: "/stats", icon: LayoutDashboard, label: "Estadísticas" }
  ];

  return (
    <aside className="w-64 glass-panel border-r border-slate-700/50 flex flex-col hidden md:flex">
      <div className="p-6">
        <h1 className="text-2xl font-bold text-gradient flex items-center gap-2">
          🏠 InmueblesApp
        </h1>
      </div>
      <nav className="flex-1 px-4 py-4 space-y-2">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = location === item.href;
          return (
            <Link key={item.href} href={item.href}>
              <a className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 ${
                isActive 
                  ? "bg-indigo-500/20 text-indigo-400 border border-indigo-500/30 shadow-[0_0_15px_rgba(99,102,241,0.2)]" 
                  : "text-slate-400 hover:bg-slate-800 hover:text-slate-200"
              }`}>
                <Icon size={20} />
                <span className="font-medium">{item.label}</span>
              </a>
            </Link>
          );
        })}
      </nav>
      <div className="p-4 m-4 rounded-xl bg-gradient-to-br from-indigo-500/10 to-purple-500/10 border border-indigo-500/20 text-sm text-slate-400">
        <p>V2 Powered by React & FastAPI</p>
      </div>
    </aside>
  );
}

function App() {
  return (
    <div className="min-h-screen bg-slate-900 flex overflow-hidden bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-slate-900 via-indigo-900/20 to-slate-900">
      <Sidebar />
      <main className="flex-1 flex flex-col h-screen overflow-y-auto overflow-x-hidden relative">
        {/* Ambient background blur */}
        <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-indigo-500/10 rounded-full blur-[120px] pointer-events-none -z-10" />
        <div className="absolute bottom-0 left-0 w-[500px] h-[500px] bg-purple-500/10 rounded-full blur-[120px] pointer-events-none -z-10" />
        
        <div className="p-8 max-w-7xl mx-auto w-full z-10">
          <Switch>
            <Route path="/" component={Dashboard} />
            <Route path="/search" component={Recommend} />
            <Route path="/predict" component={Predict} />
            <Route path="/stats" component={Dashboard} />
            <Route>
              <div className="flex flex-col items-center justify-center h-[60vh] text-center">
                <h2 className="text-4xl font-bold text-slate-300 mb-4">404 No Encontrado</h2>
                <p className="text-slate-400">La página que buscas no existe.</p>
              </div>
            </Route>
          </Switch>
        </div>
      </main>
    </div>
  );
}

export default App;

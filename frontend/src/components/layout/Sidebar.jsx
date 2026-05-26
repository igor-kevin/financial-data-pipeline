import { useTickers } from '../../hooks/useTickers'

const Sidebar = ({ tickerSelecionado, onSelect }) => {
    const { tickers, loading, error } = useTickers()

    if (loading) return <div>Carregando...</div>
    if (error) return <div>Erro ao carregar tickers</div>

    return (
        <div className="w-48 bg-gray-900 min-h-screen p-4 flex flex-col gap-2">
            <span className="text-gray-400 text-sm mb-2">Ativos</span>
            {tickers.map(ticker => (
                <button
                    key={ticker}
                    onClick={() => onSelect(ticker)}
                    className={`w-full text-left px-3 py-2 rounded text-sm transition-colors
                            ${ticker === tickerSelecionado 
                                ? 'bg-blue-600 text-white' 
                                : 'text-gray-400 hover:bg-gray-800 hover:text-white'
                            }`
                        }
                >
                    {ticker}
                </button>
            ))}
        </div>
    )
}

export default Sidebar
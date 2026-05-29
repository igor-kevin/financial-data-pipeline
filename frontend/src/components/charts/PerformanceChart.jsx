import { useComparativo } from '../../hooks/useComparativo'
import { useState } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts'

const transformarDados = (dados, ticker, periodo) => {
    return dados.map(item => ({
        date: item.date,
        [ticker]: item[`retorno_${periodo}`],
        CDI: item[`cdi_${periodo}`],
        IBOV: item[`ibov_${periodo}`]
    }))
}

const PerformanceChart = ({ticker}) => {
    const {comparativo, loading, error } = useComparativo(ticker)
    const [periodo, setPeriodo] = useState('365d')
    const periodos = [
        { chave: '30d', label: '1M' },
        { chave: '180d', label: '6M' },
        { chave: '365d', label: '1A' },
    ]

    if (loading) {
        return <div>Carregando...</div>
    }
    if (error) {
        return <div>Erro</div>
    }

    const dados = transformarDados(comparativo, ticker, periodo)

    return (
        <div>
            <div className="flex gap-2 mb-4">
                <p>Selecione o período acumulado:</p>
                {periodos.map(p => (
                    <button
                        key={p.chave}
                        onClick={() => setPeriodo(p.chave)}
                        className={`px-3 py-1 rounded text-sm ${
                            p.chave === periodo 
                                ? 'bg-blue-600 text-white' 
                                : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
                        }`}
                    >
                        {p.label}
                    </button>
                ))}
            </div>

            <ResponsiveContainer width="100%" height={400}>
                
                <LineChart data={dados}>
                    <XAxis 
                        dataKey="date" 
                        tickFormatter={(value) =>
                            new Date(value).toLocaleDateString('pt-BR')
                        }
                        />
                    <YAxis tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                    <Tooltip
                        formatter={(v) => `${(v * 100).toFixed(2)}%`} 
                        labelFormatter={(label) => `Data: ${label}`}
                        />
                    <Legend />
                    <CartesianGrid />
                    <Line type="monotone" dataKey={ticker} stroke="#3b82f6" dot={false} />
                    <Line type="monotone" dataKey="CDI" stroke="#22c55e" dot={false} />
                    <Line type="monotone" dataKey="IBOV" stroke="#f59e0b" dot={false} />
                </LineChart>
            </ResponsiveContainer>
        </div>
    )
}

export default PerformanceChart
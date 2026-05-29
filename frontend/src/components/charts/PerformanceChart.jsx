import { useComparativo } from '../../hooks/useComparativo'
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts'

const transformarDados = (dados, ticker) => {
    return dados.map(item => ({
        date: item.date,
        [ticker]: item.retorno_365d,
        CDI: item.cdi_365d,
        IBOV: item.ibov_365d
    }
    ))
}

const PerformanceChart = ({ticker}) => {
    const {comparativo, loading, error } = useComparativo(ticker)

    if (loading) {
        return <div>Carregando...</div>
    }
    if (error) {
        return <div>Erro</div>
    }

    const dados = transformarDados(comparativo, ticker)

    return (
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
    )
}

export default PerformanceChart
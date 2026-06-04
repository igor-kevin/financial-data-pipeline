import { useMetricas } from '../../hooks/useMetricas'
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts'

const DrawdownChart = ({ticker}) => {
    const {metricas, loading, error} = useMetricas(ticker)
    
    if (loading) {
        return <div> Carregando... </div>
    }
    if (error) {
        return <div> Erro ao carregar dados de drawdown </div>
    }

    const dados = metricas.map(m => ({
        date: m.date,
        valor: m.close_price,
        drawdown: m.drawdown
    }
    ))
    return (
        <div>
            <ResponsiveContainer width="100%" height={400}>
                <LineChart data={dados}>
                    <XAxis
                    dataKey ='date'
                    tickFormatter = {(value) => 
                        new Date(value).toLocaleDateString('pt-BR')
                    }
                    />
                    <YAxis yAxisId='preco' orientation='left' />
                    <YAxis yAxisId='drawdown' orientation='right' tickFormatter={(v) => `${(v*100).toFixed(0)}%`}/>
                    
                    <Line yAxisId = 'preco' dataKey='valor' stroke = '#3b82f6' dot={false}/>
                    <Line yAxisId = 'drawdown' dataKey='drawdown' stroke = '#ef4444' dot={false}/>
                    <Tooltip
                        formatter={(v) => `R$${(v).toFixed(2)}`}
                        labelFormatter={(label) => `Data: ${label}`}
                    />
                    <Legend />
                    <CartesianGrid />
                </LineChart>
            </ResponsiveContainer>
        </div>
    )
}


export default DrawdownChart
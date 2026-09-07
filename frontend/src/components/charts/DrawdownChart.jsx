import { useMetricas } from '../../hooks/useMetricas'
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid, ReferenceLine } from 'recharts'

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
        preco: m.close_price,
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
                    <YAxis yAxisId='drawdown' orientation='right' domain={[-1, 0]} tickFormatter={(v) => `${(v*100).toFixed(0)}%`}/>
                    
                    <Line yAxisId= 'preco' dataKey='preco' name='Preço'stroke= '#3b82f6' dot={false}/>
                    <Line yAxisId= 'drawdown' dataKey='drawdown' name='Drawdown' stroke= '#ef4444' dot={false}/>
                    <Tooltip
                        formatter={(value, name) => {
                            if (name === "Drawdown") {
                                return [`${(value * 100).toFixed(2)}%`, name]
                            }
                            return [`R$ ${value.toFixed(2)}`, name]
                        }}
                        labelStyle={{ color: "gray" }}
                        labelFormatter={(label) =>
                            `Data: ${new Date(label).toLocaleDateString("pt-BR")}`
                        }
                    />
                    <ReferenceLine
                        y={-0.5}
                        yAxisId="drawdown"
                    />
                    <Legend />
                    <CartesianGrid />
                    
                </LineChart>
            </ResponsiveContainer>
        </div>
    )
}


export default DrawdownChart
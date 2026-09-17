import { useForecast } from '../../hooks/useForecast'
import { useMetricas } from '../../hooks/useMetricas'
import { Area, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid, ComposedChart, ResponsiveContainer } from 'recharts'


const ForecastChart = ({ticker}) =>{
    const {forecast, loading: loadingForecast, error: errorForecast} = useForecast(ticker)
    const {metricas, loading: loadingMetricas, error: errorMetricas} = useMetricas(ticker)

    if (loadingForecast || loadingMetricas) {
        return (<div> Carregando... </div>)
    }

    if (errorForecast || errorMetricas ) {
        return (<div> Erro ao carregar o gráfico de análise de preço. </div>)
    }


    const dozeMesAtras = new Date()
    dozeMesAtras.setMonth(dozeMesAtras.getMonth()-12)

    const historico = metricas.filter(d=> new Date(d.date) >= dozeMesAtras)
        .map(d=> ({date: d.date, close_price: d.close_price, previsao: null}))

    const futuro = forecast.map( d => ({
        date: d.date,
        close_price: null,
        previsao: d.previsao,
        previsao_min: d.previsao_min,
        previsao_max: d.previsao_max,
        area: d.previsao_max - d.previsao_min
        })
    )
    
    const dados = [...historico, ...futuro].sort((a, b) => new Date(a.date) - new Date(b.date))
    const minValorForecast = Math.min(...dados.map(d => d.previsao_min).filter(Boolean))
    const maxValorForecast = Math.max(...dados.map(d => d.previsao_max).filter(Boolean))

    const minValorPreco = Math.min(...dados.map(d => d.close_price).filter(Boolean))
    const maxValorPreco = Math.max(...dados.map(d => d.close_price).filter(Boolean))

    const minValor = Math.min(minValorPreco, minValorForecast)
    const maxValor = Math.max(maxValorPreco, maxValorForecast)



    return (
        <div>
            
            <ResponsiveContainer width={'100%'} height={400}>
                <ComposedChart 
                    data={dados}
                >
                    <YAxis domain={[minValor * 0.98, maxValor * 1.02]} 
                        allowDataOverflow={true} 
                        tickFormatter={(v) => `R$${v.toFixed(2)}`}/>
                    <XAxis dataKey='date'  tickFormatter = {(value) => 
                        new Date(value).toLocaleDateString('pt-BR')
                    }/>
                    <Line dataKey='previsao' name='Previsao Preço' strokeWidth={1} stroke='#3dca38' dot={false}/>
                    <Line dataKey='previsao_min'  name='Previsao Min' strokeDasharray='3 3' strokeWidth={0.4} stroke='#50d03cf9'dot={false}/>
                    <Line dataKey='previsao_max' name='Previsao Max' strokeDasharray='3 3' strokeWidth={0.4} stroke='#61da4ee2' dot={false}/>
                    <Line dataKey={'close_price'} name={ticker} dot={false}/>
                    <Area dataKey='previsao_min'stackId={'Area'} fillOpacity={0} tooltipType='none' legendType='none' stroke='none'/>
                    <Area dataKey='area' stackId={'Area'} name='Previsao Max' strokeDasharray='3 3' legendType='none' strokeWidth={0.4} stroke='#e2dfdfae' fillOpacity={0.3} fill='#caed2ce2' tooltipType='none'/>
                    <Tooltip
                        formatter={(value, name) => {
                            return [`R$ ${value.toFixed(2)}`, name]
                        }}
                        labelStyle={{ color: "gray" }}
                        labelFormatter={(label) =>
                            `Data: ${new Date(label).toLocaleDateString("pt-BR")}`
                        }
                    />
                    <CartesianGrid />
                    <Legend />
                    
                </ComposedChart>
            </ResponsiveContainer>

        </div>
    )
}


export default ForecastChart;
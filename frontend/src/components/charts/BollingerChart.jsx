import { useBollinger } from '../../hooks/useBollinger'
import { Area, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid, ComposedChart, ResponsiveContainer } from 'recharts'


const BollingerChart = ({ticker}) =>{
    const {bollinger, loading, error} = useBollinger(ticker)
    if (loading) {
        return (<div> Carregando... </div>)
    }

    if (error) {
        return (<div> Erro ao carregar o gráfico de análise de preço. </div>)
    }
    const data= bollinger.map(item => ({
        ...item, banda: item.bb_superior_20 - item.bb_inferior_20
    }))
    const minValor = Math.min(...data.map(d => d.bb_inferior_20).filter(Boolean))
    const maxValor = Math.max(...data.map(d => d.bb_superior_20).filter(Boolean))

    return (
        <div>
            
            <ResponsiveContainer width={'100%'} height={400}>
                <ComposedChart 
                    data={data}
                >
                    <YAxis domain={[minValor * 0.98, maxValor * 1.02]} 
                        allowDataOverflow={true} 
                        tickFormatter={(v) => `R$${v.toFixed(2)}`}/>
                    <XAxis dataKey='date'  tickFormatter = {(value) => 
                        new Date(value).toLocaleDateString('pt-BR')
                    }/>
                    <Line dataKey='close_price' name='Preço' strokeWidth={1} stroke='#c3b9b9' dot={false}/>
                    <Line dataKey='ma20'  name='Media Móvel 20d' strokeWidth={0.4} stroke='#ebd727'dot={false}/>
                    <Line dataKey='ma50'  name='Media Móvel 50d' strokeWidth={0.4} stroke='#9d3232' dot={false}/>
                    <Line dataKey='bb_superior_20' strokeDasharray='3 3' stroke='#e2dfdfae' dot={false}/>
                    <Area stroke='none' dataKey={'bb_inferior_20'} fill='transparent' stackId={'bollinger'} baseValue={'minValor'}/>
                    <Area stroke='none' dataKey={'banda'} fill='#5cb219' fillOpacity={0.1} stackId={'bollinger'} baseValue={'maxValor'}/>

                    <Tooltip  />
                    <CartesianGrid />
                    <Legend />
                    
                </ComposedChart>
            </ResponsiveContainer>

        </div>
    )
}


export default BollingerChart;
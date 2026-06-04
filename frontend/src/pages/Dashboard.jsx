import { useResumo } from '../hooks/useResumo'
import SummaryCard from '../components/cards/SummaryCard'
import PerformanceChart from '../components/charts/PerformanceChart'
import DrawdownChart from '../components/charts/DrawdownChart'


const Dashboard = ({ticker}) => {
    const { resumo, loading, error } = useResumo()

    if (loading){
        return <div>Carregando...</div>
    }
    if (error){
        return <div>Erro</div>
    }

    return (
        <div className="p-6 flex flex-col gap-6">
            <div className="grid grid-cols-3 gap-4">
                {resumo.map(item => (
                    <SummaryCard key={item.ticker} data={item} />
                ))}
            </div>

            {ticker && (
                <div>
                    <PerformanceChart ticker={ticker} />
                    <DrawdownChart ticker={ticker} />
                </div>
            )}
        </div>
    )
}

export default Dashboard
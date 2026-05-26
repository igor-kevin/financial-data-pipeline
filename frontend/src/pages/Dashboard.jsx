import { useResumo } from '../hooks/useResumo'
import SummaryCard from '../components/cards/SummaryCard'

const Dashboard = () => {
    const { resumo, loading, error } = useResumo()
    if (loading) return <div>Carregando...</div>
    if (error) return <div>Erro</div>

    return (
        <div className="grid grid-cols-3 gap-4 p-6">
            {resumo.map(item => (
                <SummaryCard key={item.ticker} data={item} />
            ))}
        </div>
    )
}

export default Dashboard
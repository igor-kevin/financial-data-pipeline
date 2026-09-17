import { useResumo } from '../hooks/useResumo'
import SummaryCard from '../components/cards/SummaryCard'
import PerformanceChart from '../components/charts/PerformanceChart'
import DrawdownChart from '../components/charts/DrawdownChart'
import BollingerChart from '../components/charts/BollingerChart'
import ForecastChart from '../components/charts/ForecastChart'


const styles = {
    header : 'px-4 py-3 border-b border-gray-800 bg-blue-900',
    container: 'bg-gray-900 rounded-lg overflow-hidden',
    titulo: 'text-white font-bold text-lg',
}

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
                <div className='flex flex-col gap-8 '>
                    <section className={styles.container}>
                        <div className={styles.header}>
                            <h2 className={styles.titulo}>Performance do {ticker} VS CDI e Ibovespa</h2>
                        </div>
                        <div className='py-2'>
                            <PerformanceChart ticker={ticker} />
                        </div>
                    </section>
                    <section className={styles.container}>
                        <div className={styles.header}> 
                            <h2 className={styles.titulo}>Drawdown</h2>
                        </div>
                        <div className='py-5'>
                            <DrawdownChart ticker={ticker} />
                        </div>
                    </section>
                    <section className={styles.container}>
                        <div className={styles.header}>
                            <h2 className={styles.titulo}>Análise Técnica - Bandas de Bollinger</h2>
                        </div>
                        <div className='py-5'>
                            <BollingerChart ticker={ticker} />
                        </div>
                    </section>
                    <section className={styles.container}>
                        <div className={styles.header}>
                            <h2 className={styles.titulo}>Previsão de Preço - 30 dias</h2>
                        </div>
                        <div className='py-5'>
                            <ForecastChart ticker={ticker} />
                        </div>
                    </section>
                </div>
            )}
        </div>
    )
}

export default Dashboard
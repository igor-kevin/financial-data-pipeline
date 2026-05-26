import Header from './components/layout/Header'
import Container from './components/layout/Container'
import Sidebar from './components/layout/Sidebar'
import Dashboard from './pages/Dashboard'
import { useState } from 'react'

function App() {
    const [tickerSelecionado, setTickerSelecionado] = useState(null)

    return (
        <Container>
            <Header />
            <div className="flex">
                <Sidebar 
                    tickerSelecionado={tickerSelecionado}
                    onSelect={setTickerSelecionado}
                />
                <main className="flex-1 p-6">
                    <Dashboard ticker={tickerSelecionado}/>
                </main>
            </div>
        </Container>
    )
}

export default App;
package pipeline

import (
	"context"
	"sync"
)

// Node представляет шаг обработки данных в пайплайне.
// Может иметь N входов и M выходов, передающих данные через каналы interface{}.
type Node struct {
	In     []<-chan interface{}
	Out    []chan interface{}
	worker func(ctx context.Context, in []<-chan interface{}, out []chan interface{})
}

// Pipeline — ориентированный граф связанных узлов (Node),
// управляющий их выполнением.
type Pipeline struct {
	nodes  []*Node
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPipeline создаёт пустой пайплайн.
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// NewNode создаёт узел с numIn входами и numOut выходами.
// В worker реализуется логика: чтение из in и запись в out.
func NewNode(numIn, numOut int, worker func(ctx context.Context, in []<-chan interface{}, out []chan interface{})) *Node {
	n := &Node{
		In:     make([]<-chan interface{}, numIn),
		Out:    make([]chan interface{}, numOut),
		worker: worker,
	}
	for i := range n.Out {
		n.Out[i] = make(chan interface{})
	}
	return n
}

// AddNode добавляет узел в пайплайн.
func (p *Pipeline) AddNode(n *Node) {
	p.nodes = append(p.nodes, n)
}

// Connect соединяет src.Out[srcPort] с dst.In[dstInPort].
func (p *Pipeline) Connect(src *Node, srcPort int, dst *Node, dstInPort int) {
	dst.In[dstInPort] = src.Out[srcPort]
}

// Run запускает все узлы в отдельных горутинах.
func (p *Pipeline) Run() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for _, n := range p.nodes {
		p.wg.Add(1)
		go func(node *Node) {
			defer p.wg.Done()
			node.worker(p.ctx, node.In, node.Out)
			for _, ch := range node.Out {
				close(ch)
			}
		}(n)
	}
}

// Wait ожидает завершения всех узлов.
func (p *Pipeline) Wait() {
	p.wg.Wait()
}

// Stop сигнализирует об остановке через отмену контекста.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

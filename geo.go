package magneticdb

import (
	"fmt"
	"sync"

	"github.com/zhangpeihao/kdtree"
)

// Point provides a definition for the point
type Point struct {
	Name string
	X, Y float64
}

// Triangle
type Triangle struct {
	Name string
	X,Y,Z float64
}

// Dist struct return distance between a two items
type Dist struct {
	Result []float64
}

// Spatial: definition for the indexes
type Spatial struct {
	items map[string]*kdtree.Tree
	lock  sync.RWMutex
}

// PutPoints set a new points
func (spt *Spatial) PutPoints(name string, p []*Point) error {
	spt.lock.Lock()
	defer spt.lock.Unlock()

	// Checking, that list of the points is not empty
	if len(p) == 0 {
		return fmt.Errorf("List of the points not contains elements")
	}

	// COnstruction of the nodes
	nodes := []*kdtree.Node{}
	for _, point := range p {
		// convert point to the kd tree node
		coord := &kdtree.Coordinate{
			Values: []float64{point.X, point.Y},
		}

		nodes = append(nodes, &kdtree.Node{Coordinate: coord})
	}

	// create of the new tree
	tree, err := kdtree.NewTree(nodes, 2)
	if err != nil {
		return err
	}

	spt.items[name] = tree
	return nil
}

// PutPoints set a new points
func (spt *Spatial) PutTriangles(name string, p []*Triangle) error {
	spt.lock.Lock()
	defer spt.lock.Unlock()

	// Checking, that list of the points is not empty
	if len(p) == 0 {
		return fmt.Errorf("List of the points not contains elements")
	}

	nodes := []*kdtree.Node{}
	for _, point := range p {
		// convert point to the kd tree node
		coord := &kdtree.Coordinate{
			Values: []float64{point.X, point.Y, point.Z},
		}

		nodes = append(nodes, &kdtree.Node{Coordinate: coord})
	}

	// create of the new tree
	tree, err := kdtree.NewTree(nodes, 3)
	if err != nil {
		return err
	}

	spt.items[name] = tree
	return nil
}

// SearchPoint provides searching of teh near points on the tree
func (spt *Spatial) SearchPoints(name string, p Point, dist float64) ([]*Point, error) {
	tree, ok := spt.items[name]
	if !ok {
		return nil, fmt.Errorf("tree with the name %s is not defined", name)
	}

	// append nodes to the search points
	var retNodes []*Point
	walker := func(node *kdtree.Node) bool {
		if node == nil || node.Coordinate == nil || len(node.Coordinate.Values) != 2 {
			return false
		}
		retNodes = append(retNodes, &Point{X: node.Coordinate.Values[0], Y: node.Coordinate.Values[1]})
		return false
	}

	err := tree.Search(&kdtree.Coordinate{Values: []float64{p.X, p.Y}}, dist, walker)
	if err != nil {
		return nil, err
	}

	if len(retNodes) == 0 {
		return nil, fmt.Errorf("Not found")
	}

	return retNodes, nil
}

// Distance provides a distance between two points
func (spt *Spatial) Distance(key1, key2 string) (*Dist, error) {
	return &Dist{}, nil
}

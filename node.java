package wd1;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Node {
	public int value;
	public Set<Node> children;
	
	public Node(int value) {
		this.value = value;
		this.children = new HashSet<Node>();
	}
	
	public void addChild(Node n) {
		this.children.add(n);
	}
	
	@Override
    public int hashCode(){
        return Objects.hashCode(this.value);
    }

    @Override
    public boolean equals(Object obj){
        if ( !(obj instanceof Node) ) {
            return false;
        }
        return Objects.equals(((Node)obj).value, this.value);
    }
	
}
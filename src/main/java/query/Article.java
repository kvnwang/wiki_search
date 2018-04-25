package query;

import java.io.Serializable;

public class Article implements Serializable {
	private String url;
	private String pos;
	private String id;
	private String word;
	private String neighbor;
	private String title;

	public Article(String words, String id, String url, String neighbor, String title) {
		this.word=words;
		this.id=id;
		this.pos=null;
		this.title=title;
		this.url=url;
		if(neighbor!=null) {
			this.neighbor=neighbor.trim();			
		}
	}
	

	public String getword() {
		return word;
	}
	
	public String getPos() {
		return pos;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return this.title;
	}
	
	
	public String toString() {
		return this.word+ '\n'+title + '\n' + url + '\n' + neighbor+'\n';
	}
	
	
}

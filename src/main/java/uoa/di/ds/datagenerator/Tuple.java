package uoa.di.ds.datagenerator;

public class Tuple<C, R, A, U, I, N, S, T> {
    public final C _cpu;
    public final R _ram;
    public final A _activeSessions;
    public final U _upTime;
    public final I _id;
    public final N _name;
    public final S _site;
    public final T _temperature;

    public Tuple(C cpu, R ram, A activeSessions, U upTime, I id, N name, S site, T temperature) { 
        this._cpu = cpu;
        this._ram = ram;
        this._activeSessions = activeSessions;
        this._upTime = upTime;
        this._id = id;
        this._name = name;
        this._site = site;
        this._temperature = temperature;
    }
    
    public String toCSVString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(this._cpu);
    	sb.append(',');
    	sb.append(this._ram);
    	sb.append(',');
    	sb.append(this._activeSessions);
    	sb.append(',');
    	sb.append(this._upTime);
    	sb.append(',');
    	sb.append(this._id);
    	sb.append(',');
    	sb.append(this._name);
    	sb.append(',');
    	sb.append(this._site);
    	sb.append(',');
    	sb.append(this._temperature);
    	sb.append('\n');
    	return sb.toString();
    }
}
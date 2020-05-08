package corona.business;
public class Lab {
    int id = -1;
    String name = null;
    String city = null;
    boolean active = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean getIsActive() {
        return active;
    }

    public void setIsActive(boolean active){this.active=active;}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public static Lab badLab()
    {
        return new Lab();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Lab)) return false;

        Lab lab = (Lab) o;

        if (getId() != lab.getId()) return false;
        if (getCity() != null ? !getCity().equals(lab.getCity()) : lab.getCity() != null) return false;
        if (getIsActive() != lab.getIsActive()) return false;
        return getName() != null ? getName().equals(lab.getName()) : lab.getName() == null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Lab{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", city='").append(city).append('\'');
        sb.append(", active='").append(active).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

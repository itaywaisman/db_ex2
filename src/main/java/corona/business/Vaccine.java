package corona.business;

import java.util.Objects;


public class Vaccine {
    @Override
    public int hashCode() {
        return Objects.hash(id, name, cost, units, productivity);
    }

    int id = -1;
    String name = null;
    int cost = -1;
    int units = -1;
    int productivity = -1;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public int getUnits() {
        return units;
    }

    public void setUnits(int units) {
        this.units = units;
    }

    public int getProductivity() {
        return productivity;
    }

    public void setProductivity(int productivity) {
        this.productivity = productivity;
    }

    public static Vaccine badVaccine()
    {
        return new Vaccine();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vaccine vaccine = (Vaccine) o;
        return id == vaccine.id &&
                units == vaccine.units &&
                Objects.equals(name, vaccine.name) &&
                Objects.equals(cost, vaccine.cost) &&
                Objects.equals(productivity, vaccine.productivity);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Vaccine{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", cost='").append(cost).append('\'');
        sb.append(", units='").append(units).append('\'');
        sb.append(", productivity='").append(productivity).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

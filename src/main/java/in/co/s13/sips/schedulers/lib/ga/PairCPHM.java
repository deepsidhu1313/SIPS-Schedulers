/*
 * Copyright (C) 2018 Navdeep Singh Sidhu
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package in.co.s13.sips.schedulers.lib.ga;

import java.util.ArrayList;

/**
 *
 * @author nika
 */
public class PairCPHM {

    Processor processor;
    Chromosome chromosome;
    ArrayList<Processor> processors;

    public PairCPHM(Processor processor, Chromosome chromosome, ArrayList<Processor> processors) {
        this.processor = processor;
        this.chromosome = chromosome;
        this.processors = processors;
    }

    public Chromosome getChromosome() {
        return chromosome;
    }

    public void setChromosome(Chromosome chromosome) {
        this.chromosome = chromosome;
    }

    public ArrayList<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(ArrayList<Processor> processors) {
        this.processors = processors;
    }

    public Processor getProcessor() {
        return processor;
    }

    public void setProcessor(Processor processor) {
        this.processor = processor;
    }

    @Override
    public String toString() {
        return "PairCPHM{" + "processor=" + processor + ", chromosome=" + chromosome + ", processors=" + processors + '}';
    }

}

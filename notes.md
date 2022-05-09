import matplotlib.pyplot as plt

def random_uniform_circle(center_x, center_y, radius, random_function=np.random.uniform):
    rand_r = np.sqrt(random_function()) * radius
    rand_phi = random_function() * 2 * np.pi
    x = center_x + rand_r * np.cos(rand_phi)
    y = center_x + rand_r * np.sin(rand_phi)
    return (x, y)

center_x, center_y = 10, 10
radius = 5
plot_x = plot_y = [0, 20]

values = []
for i in range(0, 100):
    values.append(random_uniform_circle(center_x, center_y, radius))

fig = plt.figure()
ax = fig.add_subplot(111)

x, y = zip(*values)
ax.scatter(x, y)
ax.set_xlim(plot_x)
ax.set_ylim(plot_y)
